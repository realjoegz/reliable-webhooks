package io.github.realjoegz.reliablewebhooks.delivery;

import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient;
import java.util.Locale;

/**
 * Why an attempt ended as it did: the class of the status that answered it, what kept it from being
 * answered, or a lease that ran out with nothing recorded. Each reason belongs to one outcome,
 * which is the outcome that the attempt row holds.
 */
enum AttemptReason {
  HTTP_2XX("success"),
  HTTP_3XX("failure"),
  HTTP_4XX("failure"),
  HTTP_5XX("failure"),
  TIMEOUT("failure"),
  CONNECT("failure"),
  TLS("failure"),
  DNS("failure"),
  BLOCKED("failure"),
  DEADLINE("failure"),
  RESPONSE_TOO_LARGE("failure"),
  LEASE_EXPIRED("unknown");

  private final String outcome;

  AttemptReason(String outcome) {
    this.outcome = outcome;
  }

  static AttemptReason ofStatus(int status) {
    return switch (status / 100) {
      case 2 -> HTTP_2XX;
      case 3 -> HTTP_3XX;
      case 4 -> HTTP_4XX;
      // With 5xx goes every status that is none of the classes a final response can have.
      default -> HTTP_5XX;
    };
  }

  static AttemptReason ofNoResponse(WebhookHttpClient.Reason reason) {
    return switch (reason) {
      case TIMEOUT -> TIMEOUT;
      case CONNECT -> CONNECT;
      case TLS -> TLS;
      case DNS -> DNS;
      case BLOCKED -> BLOCKED;
      case DEADLINE -> DEADLINE;
      case RESPONSE_TOO_LARGE -> RESPONSE_TOO_LARGE;
    };
  }

  String outcome() {
    return outcome;
  }

  /** The reason as meters and log lines name it. */
  String label() {
    return name().toLowerCase(Locale.ROOT);
  }
}
