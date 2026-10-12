package io.github.realjoegz.reliablewebhooks.harness;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An HTTP server inside the test JVM, on the loopback address and a free port, that does with a
 * request what a test scripted for its path. It speaks HTTP over the socket by itself, because the
 * faults it plays are ones that an HTTP library would not let a server commit. A path that no test
 * scripted answers 404.
 *
 * <p>It records every request with its headers, counts the connections it accepted, and knows the
 * most requests of a path that were being answered at one time.
 */
public final class FaultReceiver implements AutoCloseable {

  private static final byte[] LOOPBACK = {127, 0, 0, 1};
  private static final int BACKLOG = 1024;
  private static final int MAX_HEAD_BYTES = 64 * 1024;
  private static final String END_OF_HEAD = "\r\n\r\n";

  /** A request as it arrived. The names of the headers are in lower case. */
  public record Request(String method, String path, Map<String, String> headers, byte[] body) {

    public String header(String name) {
      return headers.get(name.toLowerCase(Locale.ROOT));
    }
  }

  /** What the receiver does with a request. */
  public abstract static class Script {

    private Script() {}

    /** Returns whether the connection can take another request afterwards. */
    abstract boolean answer(OutputStream out, FaultReceiver receiver)
        throws IOException, InterruptedException;
  }

  /** Requests wait here until the hold is released, and are then answered as scripted. */
  public static final class Hold extends Script implements AutoCloseable {

    private final Script then;
    private final CountDownLatch released = new CountDownLatch(1);
    private final AtomicInteger waiting = new AtomicInteger();

    private Hold(Script then) {
      this.then = then;
    }

    /** The number of requests that are waiting right now. */
    public int waiting() {
      return waiting.get();
    }

    public void release() {
      released.countDown();
    }

    @Override
    public void close() {
      release();
    }

    @Override
    boolean answer(OutputStream out, FaultReceiver receiver)
        throws IOException, InterruptedException {
      waiting.incrementAndGet();
      try {
        released.await();
      } finally {
        waiting.decrementAndGet();
      }
      return then.answer(out, receiver);
    }
  }

  /**
   * A response with a part that never ends. The receiver writes it until the connection is gone,
   * and then knows how much it wrote and which side closed the connection.
   */
  public static final class Endless extends Script {

    /**
     * More than the buffers between the receiver and a peer that has stopped reading can take. A
     * response that its peer cut has written less than this. One that its peer read until a
     * deadline of seconds has written many times as much.
     */
    public static final long MORE_THAN_BUFFERS_TAKE = 16L * 1024 * 1024;

    private static final String FILL = "a".repeat(1024);

    private final byte[] start;
    private final byte[] repeated;
    private final AtomicLong bytesWritten = new AtomicLong();
    private volatile Boolean peerClosedFirst;

    private Endless(String start, String repeated) {
      this.start = start.getBytes(StandardCharsets.US_ASCII);
      this.repeated = repeated.getBytes(StandardCharsets.US_ASCII);
    }

    public long bytesWritten() {
      return bytesWritten.get();
    }

    /** Whether the response has stopped, which it does only when its connection is gone. */
    public boolean ended() {
      return peerClosedFirst != null;
    }

    /** Whether it was the peer, and not the closing of this receiver, that ended the response. */
    public boolean peerClosedFirst() {
      if (peerClosedFirst == null) {
        throw new IllegalStateException("The response has not ended");
      }
      return peerClosedFirst;
    }

    @Override
    boolean answer(OutputStream out, FaultReceiver receiver) {
      try {
        out.write(start);
        bytesWritten.addAndGet(start.length);
        while (true) {
          out.write(repeated);
          bytesWritten.addAndGet(repeated.length);
        }
      } catch (IOException connectionGone) {
        peerClosedFirst = !receiver.closing;
        return false;
      }
    }
  }

  /** Answers with the status, the headers given as names and values in turn, and no body. */
  public static Script status(int status, String... headers) {
    if (headers.length % 2 != 0) {
      throw new IllegalArgumentException("Headers are given as names and values in turn");
    }
    StringBuilder response = new StringBuilder("HTTP/1.1 %d Scripted\r\n".formatted(status));
    for (int i = 0; i < headers.length; i += 2) {
      response.append(headers[i]).append(": ").append(headers[i + 1]).append("\r\n");
    }
    response.append("Content-Length: 0").append(END_OF_HEAD);
    return whole(response.toString());
  }

  /** Answers 200 with a body of that many bytes, whose length the head declares. */
  public static Script body(int length) {
    return whole(
        "HTTP/1.1 200 Scripted\r\nContent-Length: " + length + END_OF_HEAD + "a".repeat(length));
  }

  public static Script redirect(String location) {
    return status(302, "Location", location);
  }

  /** Holds every request it gets until it is released, and then answers as the other script. */
  public static Hold hold(Script then) {
    return new Hold(then);
  }

  /** Answers 200, one byte of the response at a time with the interval between them. */
  public static Script drip(Duration interval) {
    byte[] response =
        ("HTTP/1.1 200 Scripted\r\nContent-Length: 0" + END_OF_HEAD)
            .getBytes(StandardCharsets.US_ASCII);
    return new Script() {
      @Override
      boolean answer(OutputStream out, FaultReceiver receiver)
          throws IOException, InterruptedException {
        for (byte next : response) {
          Thread.sleep(interval);
          out.write(next);
          out.flush();
        }
        return true;
      }
    };
  }

  /** Closes the connection without a byte of response. */
  public static Script closeWithoutResponse() {
    return new Script() {
      @Override
      boolean answer(OutputStream out, FaultReceiver receiver) {
        return false;
      }
    };
  }

  /** Answers with a status line and one header line that never ends. */
  public static Endless endlessHeader() {
    return new Endless("HTTP/1.1 200 Scripted\r\nX-Endless: ", Endless.FILL);
  }

  /**
   * Answers 200 with a body that never ends. The head declares no length and no chunks, so the body
   * is all that the connection carries until it closes.
   */
  public static Endless endlessBody() {
    return new Endless(
        "HTTP/1.1 200 Scripted\r\nContent-Type: application/octet-stream" + END_OF_HEAD,
        Endless.FILL);
  }

  /**
   * Answers 200 with a body in chunks that never ends. A body in chunks is one that a client can
   * read to its end to use the connection again, and a client that did would read this one for as
   * long as it is allowed to.
   */
  public static Endless endlessChunkedBody() {
    return new Endless(
        "HTTP/1.1 200 Scripted\r\nContent-Type: application/octet-stream\r\n"
            + "Transfer-Encoding: chunked"
            + END_OF_HEAD,
        Integer.toHexString(Endless.FILL.length()) + "\r\n" + Endless.FILL + "\r\n");
  }

  // A response that is written in one piece. The connection can take another request after it.
  private static Script whole(String response) {
    byte[] bytes = response.getBytes(StandardCharsets.ISO_8859_1);
    return new Script() {
      @Override
      boolean answer(OutputStream out, FaultReceiver receiver) throws IOException {
        out.write(bytes);
        out.flush();
        return true;
      }
    };
  }

  private final ServerSocket server;
  private final ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor();
  private final Set<Socket> connections = ConcurrentHashMap.newKeySet();
  private final AtomicInteger accepted = new AtomicInteger();
  private final List<Request> requests = new CopyOnWriteArrayList<>();
  private final Map<String, Script> scripts = new ConcurrentHashMap<>();
  private final Map<String, Queue<Script>> scriptsForOneRequest = new ConcurrentHashMap<>();
  private final Map<String, Integer> beingAnswered = new ConcurrentHashMap<>();
  private final Map<String, Integer> peaks = new ConcurrentHashMap<>();
  private volatile boolean closing;

  private FaultReceiver(ServerSocket server) {
    this.server = server;
  }

  public static FaultReceiver start() throws IOException {
    FaultReceiver receiver =
        new FaultReceiver(new ServerSocket(0, BACKLOG, InetAddress.getByAddress(LOOPBACK)));
    receiver.threads.execute(receiver::accept);
    return receiver;
  }

  public int port() {
    return server.getLocalPort();
  }

  /** The URL of a path of this receiver, by its address and not by a name. */
  public String url(String path) {
    return "http://127.0.0.1:%d%s".formatted(port(), path);
  }

  /** What every request to the path gets for which no script for one request is left. */
  public void answer(String path, Script script) {
    scripts.put(path, script);
  }

  /** What one request to the path gets. Several of them are used in the order scripted. */
  public void answerOnce(String path, Script script) {
    scriptsForOneRequest
        .computeIfAbsent(path, unscripted -> new ConcurrentLinkedQueue<>())
        .add(script);
  }

  /** Every request so far, in the order in which they arrived. */
  public List<Request> requests() {
    return List.copyOf(requests);
  }

  public List<Request> requests(String path) {
    return requests.stream().filter(request -> request.path().equals(path)).toList();
  }

  public int acceptedConnections() {
    return accepted.get();
  }

  /** The most requests to the path that were being answered at the same time. */
  public int peakConcurrency(String path) {
    return peaks.getOrDefault(path, 0);
  }

  /** Stops listening and closes every connection, also those that a script still writes to. */
  @Override
  public void close() throws IOException {
    closing = true;
    server.close();
    for (Socket connection : connections) {
      connection.close();
    }
    threads.shutdownNow();
  }

  private void accept() {
    while (!closing) {
      try {
        Socket connection = server.accept();
        accepted.incrementAndGet();
        connections.add(connection);
        threads.execute(() -> serve(connection));
      } catch (IOException closed) {
        return;
      }
    }
  }

  private void serve(Socket connection) {
    try (connection) {
      InputStream in = new BufferedInputStream(connection.getInputStream());
      OutputStream out = connection.getOutputStream();
      boolean reusable = true;
      while (reusable) {
        Request request = read(in);
        if (request == null) {
          return;
        }
        requests.add(request);
        reusable = answer(request.path(), out);
      }
    } catch (IOException | InterruptedException connectionGoneOrReceiverClosed) {
      // Either ends the connection, which the try block has closed.
    } finally {
      connections.remove(connection);
    }
  }

  private boolean answer(String path, OutputStream out) throws IOException, InterruptedException {
    peaks.merge(path, beingAnswered.merge(path, 1, Integer::sum), Math::max);
    try {
      return scriptFor(path).answer(out, this);
    } finally {
      beingAnswered.merge(path, -1, Integer::sum);
    }
  }

  private Script scriptFor(String path) {
    Queue<Script> forOneRequest = scriptsForOneRequest.get(path);
    Script script = forOneRequest == null ? null : forOneRequest.poll();
    if (script == null) {
      script = scripts.get(path);
    }
    return script == null ? status(404) : script;
  }

  // Null when the peer closed the connection where a request would begin. A first byte that is no
  // letter is no HTTP request, for example the start of a TLS handshake, and ends the connection.
  private static Request read(InputStream in) throws IOException {
    ByteArrayOutputStream head = new ByteArrayOutputStream();
    int matched = 0;
    while (matched < END_OF_HEAD.length()) {
      int next = in.read();
      if (next == -1) {
        if (head.size() == 0) {
          return null;
        }
        throw new EOFException("The connection ended inside a request head");
      }
      if (head.size() == 0 && !Character.isLetter(next)) {
        throw new IOException("Not an HTTP request");
      }
      head.write(next);
      if (head.size() > MAX_HEAD_BYTES) {
        throw new IOException("The request head is too long");
      }
      matched = next == END_OF_HEAD.charAt(matched) ? matched + 1 : next == '\r' ? 1 : 0;
    }

    String[] lines = head.toString(StandardCharsets.ISO_8859_1).split("\r\n");
    String[] requestLine = lines[0].split(" ");
    if (requestLine.length != 3) {
      throw new IOException("Not an HTTP request line: " + lines[0]);
    }
    Map<String, String> headers = new LinkedHashMap<>();
    for (int i = 1; i < lines.length; i++) {
      int colon = lines[i].indexOf(':');
      headers.put(
          lines[i].substring(0, colon).toLowerCase(Locale.ROOT),
          lines[i].substring(colon + 1).strip());
    }
    int length = Integer.parseInt(headers.getOrDefault("content-length", "0"));
    byte[] body = in.readNBytes(length);
    if (body.length < length) {
      throw new EOFException("The connection ended inside a request body");
    }
    return new Request(requestLine[0], requestLine[1], Map.copyOf(headers), body);
  }
}
