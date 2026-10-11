package io.github.realjoegz.reliablewebhooks.harness;

import io.github.realjoegz.reliablewebhooks.http.HostResolver;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

/**
 * The resolver of every replica that runs inside the test JVM, in place of the resolver of the
 * operating system, so that no test depends on real DNS. A name resolves only as a test scripted
 * it. An address literal answers with itself, as it does with the real resolver.
 */
@Component
@Primary
public final class StubDns implements HostResolver {

  private static final Pattern IPV4_LITERAL = Pattern.compile("\\d{1,3}(\\.\\d{1,3}){3}");

  private final Map<String, Queue<InetAddress[]>> answersForOneCall = new ConcurrentHashMap<>();
  private final Map<String, InetAddress[]> answers = new ConcurrentHashMap<>();
  private final Map<String, Hold> holds = new ConcurrentHashMap<>();

  /** The answer to every lookup of the host for which no single-call answer is left. */
  public void answer(String host, String... addresses) {
    answers.put(host, parse(addresses));
  }

  /** The answer to one lookup of the host. Several of them are given in the order scripted. */
  public void answerOnce(String host, String... addresses) {
    answersForOneCall
        .computeIfAbsent(host, unscripted -> new ConcurrentLinkedQueue<>())
        .add(parse(addresses));
  }

  public Hold hold(String host) {
    Hold hold = new Hold(host);
    holds.put(host, hold);
    return hold;
  }

  @Override
  public InetAddress[] resolve(String host) throws UnknownHostException {
    Hold hold = holds.get(host);
    if (hold != null) {
      hold.await();
    }
    if (host.startsWith("[") || IPV4_LITERAL.matcher(host).matches()) {
      return new InetAddress[] {InetAddress.getByName(host)};
    }
    Queue<InetAddress[]> forOneCall = answersForOneCall.get(host);
    InetAddress[] answer = forOneCall == null ? null : forOneCall.poll();
    if (answer == null) {
      answer = answers.get(host);
    }
    if (answer == null) {
      throw new UnknownHostException(host);
    }
    return answer;
  }

  private static InetAddress[] parse(String... addresses) {
    if (addresses.length == 0) {
      throw new IllegalArgumentException("An answer has at least one address");
    }
    InetAddress[] parsed = new InetAddress[addresses.length];
    for (int i = 0; i < addresses.length; i++) {
      try {
        // An address literal is parsed, not looked up.
        parsed[i] = InetAddress.getByName(addresses[i]);
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException(addresses[i], e);
      }
    }
    return parsed;
  }

  /** Lookups of a held host block until the hold is closed, and then answer as scripted. */
  public final class Hold implements AutoCloseable {

    private final String host;
    private final CountDownLatch released = new CountDownLatch(1);
    private final AtomicInteger waiting = new AtomicInteger();

    private Hold(String host) {
      this.host = host;
    }

    /** The number of lookups that are blocked right now. */
    public int waiting() {
      return waiting.get();
    }

    @Override
    public void close() {
      holds.remove(host, this);
      released.countDown();
    }

    private void await() throws UnknownHostException {
      waiting.incrementAndGet();
      try {
        released.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new UnknownHostException(host);
      } finally {
        waiting.decrementAndGet();
      }
    }
  }
}
