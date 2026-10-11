package io.github.realjoegz.reliablewebhooks.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

import io.github.realjoegz.reliablewebhooks.harness.FaultReceiver;
import io.github.realjoegz.reliablewebhooks.harness.ReplicaWorker;
import io.github.realjoegz.reliablewebhooks.harness.Rows;
import io.github.realjoegz.reliablewebhooks.harness.TestTenant;
import io.github.realjoegz.reliablewebhooks.harness.UnsafeLocalReplica;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * What a subscriber can do to the replica with its response, seen in the attempt rows and at the
 * receiver. In every test the endpoints of two tenants are paths of one receiver, so the one client
 * of the replica sends the requests of both to the same host and port.
 */
@UnsafeLocalReplica
class OutboundRequestIT {

  private static final String EVENT =
      """
      {"type":"order.created","data":{"order":17}}""";
  private static final Duration LONGEST_WAIT_AFTER_A_FIRST_FAILURE = Duration.ofSeconds(10);

  @Autowired RestTestClient http;
  @Autowired ReplicaWorker worker;

  // Only the limit on a header line can end this read before the deadline does, and the attempt
  // is recorded as a head that is too large.
  @Test
  void endlessHeaderIsCutAtLimit() throws Exception {
    assertResponseIsCutAtItsLimit(
        "endless-header",
        FaultReceiver.endlessHeader(),
        Rows.Attempt.unanswered(1, "response_too_large", "127.0.0.1"),
        new Rows.Delivery("pending", 1, 0, false, null, false));
  }

  // The status line decides, so the delivery has succeeded. The body is no part of that.
  @Test
  void endlessBodyIsNotDrained() throws Exception {
    assertResponseIsCutAtItsLimit(
        "endless-body",
        FaultReceiver.endlessBody(),
        Rows.Attempt.answered(1, 200),
        new Rows.Delivery("succeeded", 1, 0, false, null, true));
  }

  // The receiver answers the first tenant with 503 and a cookie for every path of its host. The
  // client call of an attempt has returned by the time the attempt is recorded, so a client that
  // sent a request again after a 503 would have sent the second one by then.
  //
  // Two requests follow the cookie: one of the second tenant, and the second attempt of the
  // delivery that was answered with it. Which of the two arrives first says nothing about the
  // cookie, so the requests are compared without their order.
  @Test
  void oneRequestPerAttemptAndNoCookieReuse() throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start()) {
      receiver.answer(
          "/first-tenant",
          FaultReceiver.status(503, "Set-Cookie", "session=of-the-first-tenant; Path=/"));
      receiver.answer("/second-tenant", FaultReceiver.status(200));
      TestTenant first = TestTenant.create(http, "answered-with-a-cookie");
      first.register(http, receiver.url("/first-tenant"), "*");
      TestTenant second = TestTenant.create(http, "beside-the-cookie");
      second.register(http, receiver.url("/second-tenant"), "*");
      worker.start();

      UUID rejected = Rows.deliveryOf(first.accept(http, EVENT));
      await().untilAsserted(() -> assertThat(Rows.attempts(rejected)).hasSize(1));
      assertThat(receiver.requests()).hasSize(1);

      UUID delivered = Rows.deliveryOf(second.accept(http, EVENT));
      await()
          .untilAsserted(() -> assertThat(Rows.delivery(delivered).state()).isEqualTo("succeeded"));
      Rows.ageDelivery(rejected, LONGEST_WAIT_AFTER_A_FIRST_FAILURE);
      await().untilAsserted(() -> assertThat(Rows.attempts(rejected)).hasSize(2));

      assertThat(Rows.attempts(rejected))
          .containsExactly(Rows.Attempt.answered(1, 503), Rows.Attempt.answered(2, 503));
      assertThat(Rows.attempts(delivered)).containsExactly(Rows.Attempt.answered(1, 200));
      assertThat(receiver.requests())
          .extracting(FaultReceiver.Request::path, request -> request.header("webhook-id"))
          .containsExactlyInAnyOrder(
              tuple("/first-tenant", rejected.toString()),
              tuple("/second-tenant", delivered.toString()),
              tuple("/first-tenant", rejected.toString()));
      assertThat(receiver.requests())
          .allSatisfy(request -> assertThat(request.headers()).doesNotContainKey("cookie"));
    }
  }

  // The request of another tenant is in flight to the same receiver before the endless response
  // begins, and the receiver holds it until that response has been cut. It is answered then, and
  // its delivery succeeds: cutting one response costs one connection and no other request.
  //
  // The receiver writes until its connection is gone. That the client closed it, and how little
  // had been written by then, shows that the response was cut and not read to the deadline.
  private void assertResponseIsCutAtItsLimit(
      String name,
      FaultReceiver.Endless endless,
      Rows.Attempt expectedAttempt,
      Rows.Delivery expectedDelivery)
      throws Exception {
    try (FaultReceiver receiver = FaultReceiver.start();
        FaultReceiver.Hold hold = FaultReceiver.hold(FaultReceiver.status(200))) {
      receiver.answer("/endless", endless);
      receiver.answer("/other-tenant", hold);
      TestTenant tenant = TestTenant.create(http, name);
      tenant.register(http, receiver.url("/endless"), "*");
      TestTenant other = TestTenant.create(http, "beside-" + name);
      other.register(http, receiver.url("/other-tenant"), "*");
      worker.start();
      UUID ofTheOtherTenant = Rows.deliveryOf(other.accept(http, EVENT));
      await().until(() -> hold.waiting() == 1);

      UUID delivery = Rows.deliveryOf(tenant.accept(http, EVENT));
      await().untilAsserted(() -> assertThat(Rows.attempts(delivery)).hasSize(1));

      assertThat(Rows.attempts(delivery)).containsExactly(expectedAttempt);
      assertThat(Rows.delivery(delivery)).isEqualTo(expectedDelivery);
      await().until(endless::ended);
      assertThat(endless.peerClosedFirst()).isTrue();
      assertThat(endless.bytesWritten()).isLessThan(FaultReceiver.Endless.MORE_THAN_BUFFERS_TAKE);

      assertThat(hold.waiting()).isEqualTo(1);
      hold.release();
      await()
          .untilAsserted(
              () -> assertThat(Rows.delivery(ofTheOtherTenant).state()).isEqualTo("succeeded"));
      assertThat(Rows.attempts(ofTheOtherTenant)).containsExactly(Rows.Attempt.answered(1, 200));
    }
  }
}
