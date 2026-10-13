import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntConsumer;

/**
 * The subscriber of the load tests: one file without a build, started with {@code java Sink.java
 * <port>}. It answers a delivery at once unless it was told to wait, with 200 unless it was told
 * another status, and counts the requests that it received for each {@code webhook-id}, which is
 * how a run knows how many deliveries arrived more than once.
 *
 * <p>It checks no signature and prints nothing, so that the replicas are measured and not their
 * subscriber.
 *
 * <ul>
 *   <li>{@code GET /stats} answers with the counts, and {@code DELETE /stats} forgets them, which a
 *       run asks for between its warm-up and its scenario.
 *   <li>{@code PUT /status} takes the status that a delivery is answered with from then on. With
 *       503 it is a subscriber that is down.
 *   <li>{@code PUT /delay} takes the milliseconds that it waits from then on before it answers a
 *       delivery. With 50 it is a subscriber that does some work for a request.
 * </ul>
 */
public final class Sink {

  private static final ConcurrentHashMap<String, Integer> requestsById = new ConcurrentHashMap<>();
  private static final LongAdder requests = new LongAdder();
  private static volatile int statusOfAnswer = 200;
  private static volatile int delayMillis;

  private Sink() {}

  public static void main(String[] args) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(Integer.parseInt(args[0])), 0);
    server.createContext("/", Sink::receive);
    server.createContext("/stats", Sink::stats);
    server.createContext(
        "/status",
        exchange -> take(exchange, "[2-5][0-9][0-9]", status -> statusOfAnswer = status));
    server.createContext(
        "/delay", exchange -> take(exchange, "[0-9]{1,4}", millis -> delayMillis = millis));
    server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    server.start();
  }

  private static void receive(HttpExchange exchange) throws IOException {
    try (exchange) {
      // Read to its end, so that the connection can carry the next request.
      exchange.getRequestBody().readAllBytes();
      String id = exchange.getRequestHeaders().getFirst("webhook-id");
      if (id == null) {
        exchange.sendResponseHeaders(400, -1);
        return;
      }
      requests.increment();
      requestsById.merge(id, 1, Integer::sum);
      Thread.sleep(delayMillis);
      exchange.sendResponseHeaders(statusOfAnswer, -1);
    } catch (InterruptedException stopped) {
      Thread.currentThread().interrupt();
    }
  }

  /** Takes a setting, which is the body of its request, when it has the form of one. */
  private static void take(HttpExchange exchange, String form, IntConsumer setting)
      throws IOException {
    try (exchange) {
      String given = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
      if (!given.matches(form)) {
        exchange.sendResponseHeaders(400, -1);
        return;
      }
      setting.accept(Integer.parseInt(given));
      exchange.sendResponseHeaders(204, -1);
    }
  }

  private static void stats(HttpExchange exchange) throws IOException {
    try (exchange) {
      if (exchange.getRequestMethod().equals("DELETE")) {
        requestsById.clear();
        requests.reset();
        exchange.sendResponseHeaders(204, -1);
        return;
      }
      long moreThanOnce = requestsById.values().stream().filter(count -> count > 1).count();
      byte[] answer =
          "{\"requests\": %d, \"webhook_ids\": %d, \"received_more_than_once\": %d}%n"
              .formatted(requests.sum(), requestsById.size(), moreThanOnce)
              .getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, answer.length);
      exchange.getResponseBody().write(answer);
    }
  }
}
