package io.github.realjoegz.reliablewebhooks.http;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLException;
import org.apache.hc.client5.http.ConnectTimeoutException;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.ManagedHttpClientConnectionFactory;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.EndpointDetails;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.MessageConstraintException;
import org.apache.hc.core5.http.config.Http1Config;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.io.Closer;
import org.apache.hc.core5.util.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Sends webhook requests, and is the only class that builds an HTTP client. One client serves every
 * tenant, so it carries nothing from one request to the next: no cookies, no cached authentication,
 * no redirect that it follows and no request that it repeats by itself. A call is one request.
 *
 * <p>Every connection is made to an address that {@link DnsLookups} has checked. A request with all
 * of its response ends inside the attempt deadline, and no more of a response is read than the
 * limits below allow.
 */
@Component
public class WebhookHttpClient implements AutoCloseable {

  static final int MAX_LINE_BYTES = 8 * 1024;
  static final int MAX_HEADERS = 100;
  static final int MAX_BODY_BYTES = 64 * 1024;

  private static final Timeout CONNECT_TIMEOUT = Timeout.ofSeconds(3);
  private static final String RETRY_AFTER = "Retry-After";

  // The default would name the HTTP library and the Java version to every subscriber.
  private static final String USER_AGENT = "reliable-webhooks";

  // JSON is UTF-8 by definition and its media type has no charset parameter.
  private static final ContentType JSON = ContentType.create("application/json");

  public sealed interface Outcome permits Response, Failed, LookupBusy {}

  /**
   * The head of a response arrived. The status is all that an attempt is judged by: what follows
   * the head is read to let the connection be used again, and is no part of the outcome.
   */
  public record Response(int status, String retryAfter, InetAddress remoteAddress)
      implements Outcome {}

  /** No response head arrived. The address is null unless a connection had been made. */
  public record Failed(Reason reason, InetAddress remoteAddress) implements Outcome {}

  public enum Reason {
    /** The lookup of the host did not answer in time. */
    TIMEOUT,
    /**
     * No connection could be made, or the connection ended before it had carried a response head.
     */
    CONNECT,
    TLS,
    /** The host does not resolve. */
    DNS,
    /** The host resolves to an address that the policy denies. */
    BLOCKED,
    /** The attempt deadline passed before the head of a response had arrived. */
    DEADLINE,
    /**
     * The status line or a header line takes more than 8 KiB with its line end, or there are more
     * than 100 headers.
     */
    RESPONSE_TOO_LARGE
  }

  /** This replica did not admit the lookup of the host, so nothing was sent. */
  public record LookupBusy() implements Outcome {}

  private final SsrfGuardDnsResolver resolver;
  private final Duration deadline;
  private final CloseableHttpClient client;
  private final ScheduledThreadPoolExecutor watchdog =
      new ScheduledThreadPoolExecutor(
          1, Thread.ofPlatform().daemon().name("attempt-deadline").factory());

  @Autowired
  public WebhookHttpClient(DnsLookups lookups, WebhooksProperties properties) {
    this(lookups, properties.delivery().attemptDeadline(), properties.delivery().maxInFlight());
  }

  WebhookHttpClient(DnsLookups lookups, Duration deadline, int maxConnections) {
    this.resolver = new SsrfGuardDnsResolver(lookups);
    this.deadline = deadline;
    this.watchdog.setRemoveOnCancelPolicy(true);

    // The watchdog is what ends a request at the deadline. These timeouts are the same bound
    // again, for a wait that the watchdog did not end.
    Timeout bound = Timeout.of(deadline);
    // The library refuses a head once it has read as many headers as its limit, so its limit is
    // one above the number that is allowed.
    Http1Config headLimits =
        Http1Config.custom()
            .setMaxLineLength(MAX_LINE_BYTES)
            .setMaxHeaderCount(MAX_HEADERS + 1)
            .build();
    // One connection for each attempt that a replica can run, so that no attempt waits for one.
    PoolingHttpClientConnectionManager connections =
        PoolingHttpClientConnectionManagerBuilder.create()
            .setDnsResolver(resolver)
            .setConnectionFactory(
                ManagedHttpClientConnectionFactory.builder().http1Config(headLimits).build())
            .setDefaultConnectionConfig(
                ConnectionConfig.custom()
                    .setConnectTimeout(CONNECT_TIMEOUT)
                    .setSocketTimeout(bound)
                    .build())
            .setMaxConnTotal(maxConnections)
            .setMaxConnPerRoute(maxConnections)
            .build();
    // The builder reads no system properties and plans no proxy unless it is told to.
    this.client =
        HttpClientBuilder.create()
            .setConnectionManager(connections)
            .setDefaultRequestConfig(
                RequestConfig.custom()
                    .setConnectionRequestTimeout(bound)
                    .setResponseTimeout(bound)
                    .build())
            .setUserAgent(USER_AGENT)
            .disableCookieManagement()
            .disableAutomaticRetries()
            .disableAuthCaching()
            .disableContentCompression()
            .disableRedirectHandling()
            .build();
  }

  /** Sends the body as one JSON POST request with the given headers. */
  public Outcome send(UUID tenantId, URI url, Map<String, String> headers, byte[] body) {
    HttpPost request = new HttpPost(url);
    headers.forEach(request::setHeader);
    request.setEntity(new ByteArrayEntity(body, JSON));
    HttpClientContext context = HttpClientContext.create();

    AtomicBoolean deadlinePassed = new AtomicBoolean();
    ScheduledFuture<?> cut =
        watchdog.schedule(
            () -> {
              deadlinePassed.set(true);
              request.abort();
            },
            deadline.toNanos(),
            TimeUnit.NANOSECONDS);
    resolver.resolveFor(tenantId);
    try {
      return exchange(request, context);
    } catch (SsrfGuardDnsResolver.Refused refused) {
      return switch (refused.refusal()) {
        case BUSY -> new LookupBusy();
        case TIMEOUT -> new Failed(Reason.TIMEOUT, null);
        case FAILED -> new Failed(Reason.DNS, null);
        case BLOCKED -> new Failed(Reason.BLOCKED, null);
      };
    } catch (IOException failure) {
      InetAddress remoteAddress = remoteAddressOf(context);
      Reason reason =
          deadlinePassed.get() ? Reason.DEADLINE : reasonOf(failure, remoteAddress != null);
      return new Failed(reason, remoteAddress);
    } finally {
      resolver.clear();
      cut.cancel(false);
    }
  }

  @Override
  public void close() {
    client.close(CloseMode.GRACEFUL);
    watchdog.shutdownNow();
  }

  private Outcome exchange(HttpPost request, HttpClientContext context) throws IOException {
    ClassicHttpResponse response = client.executeOpen(null, request, context);
    try {
      Header retryAfter = response.getFirstHeader(RETRY_AFTER);
      Outcome outcome =
          new Response(
              response.getCode(),
              retryAfter == null ? null : retryAfter.getValue(),
              remoteAddressOf(context));
      // A body that ends inside the limit leaves a connection that the next request can use. Any
      // other body has its connection closed under it, by the abort or by the watchdog. Nothing
      // waits for the rest of it.
      try {
        if (!endsWithin(response.getEntity(), MAX_BODY_BYTES)) {
          request.abort();
        }
      } catch (IOException brokenBody) {
        request.abort();
      }
      return outcome;
    } finally {
      // Closing reads what is left of a body, which fails at once on an aborted connection.
      Closer.closeQuietly(response);
    }
  }

  private static boolean endsWithin(HttpEntity entity, int limit) throws IOException {
    if (entity == null) {
      return true;
    }
    InputStream content = entity.getContent();
    byte[] buffer = new byte[8 * 1024];
    int seen = 0;
    while (seen <= limit) {
      int read = content.read(buffer, 0, Math.min(buffer.length, limit + 1 - seen));
      if (read == -1) {
        return true;
      }
      seen += read;
    }
    return false;
  }

  // A read waits as long as the deadline at most, and begins after the attempt has. A read that
  // timed out on a connection that carried the request has therefore outlived the deadline. Before
  // that, the only read is the one of a TLS handshake, which has the shorter connect timeout.
  private static Reason reasonOf(IOException failure, boolean requestSent) {
    return switch (failure) {
      case MessageConstraintException headTooLarge -> Reason.RESPONSE_TOO_LARGE;
      case SSLException tls -> Reason.TLS;
      case ConnectTimeoutException noConnection -> Reason.CONNECT;
      case SocketTimeoutException silence -> requestSent ? Reason.DEADLINE : Reason.TLS;
      default -> Reason.CONNECT;
    };
  }

  private static InetAddress remoteAddressOf(HttpClientContext context) {
    EndpointDetails endpoint = context.getEndpointDetails();
    return endpoint != null && endpoint.getRemoteAddress() instanceof InetSocketAddress remote
        ? remote.getAddress()
        : null;
  }
}
