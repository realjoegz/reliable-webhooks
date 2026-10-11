package io.github.realjoegz.reliablewebhooks.ingest;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Optional;
import java.util.UUID;
import tools.jackson.core.JacksonException;
import tools.jackson.core.StreamReadConstraints;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.cfg.JsonNodeFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * An event as its producer sent it: a type, and the data that goes to the subscribers. No number of
 * the data is read into a double, so a value too large or too precise for one reaches the
 * subscribers exact.
 */
public final class EventRequest {

  // The limits on a body. They are set here and not left to the defaults of the library, which
  // a new version of it may change. A number is counted by its digits, those of its exponent
  // included. A member name is counted by its UTF-8 bytes. The body itself is the first level.
  //
  // The limit on a name is far below the 50,000 bytes of the library. The parser keeps the names
  // it has read in a table that all requests share, so a name outlives its request, and the
  // limit is what bounds the size of an entry of that table.
  private static final int MAX_NUMBER_DIGITS = 1000;
  private static final int MAX_NAME_BYTES = 1024;
  private static final int MAX_NESTING_DEPTH = 500;

  // Not the mapper of the API: a change to how responses are written must not change the bytes
  // that subscribers receive.
  private static final JsonMapper JSON =
      JsonMapper.builder(
              JsonFactory.builder()
                  .streamReadConstraints(
                      StreamReadConstraints.builder()
                          .maxNumberLength(MAX_NUMBER_DIGITS)
                          .maxNameLength(MAX_NAME_BYTES)
                          .maxNestingDepth(MAX_NESTING_DEPTH)
                          .build())
                  .build())
          .enable(JsonNodeFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .build();

  // Instant.toString() leaves the fraction out on a whole second. Every body has three digits.
  private static final DateTimeFormatter MILLISECONDS =
      new DateTimeFormatterBuilder().appendInstant(3).toFormatter();

  private final String type;
  private final JsonNode data;
  private final byte[] hash;

  private EventRequest(String type, JsonNode data, byte[] hash) {
    this.type = type;
    this.data = data;
    this.hash = hash;
  }

  /**
   * Empty unless the body is one JSON object inside the limits, with an event type as {@code type}
   * and an object as {@code data}. Other members are ignored. Of members with the same name in one
   * object, the last one counts.
   */
  public static Optional<EventRequest> parse(byte[] body) {
    JsonNode root;
    try {
      root = JSON.readTree(body);
    } catch (JacksonException malformed) {
      return Optional.empty();
    } catch (NumberFormatException unrepresentable) {
      // What Jackson throws for a number whose exponent is beyond what a BigDecimal can hold.
      return Optional.empty();
    }
    JsonNode type = root.get("type");
    JsonNode data = root.get("data");
    if (type == null
        || !type.isString()
        || !EventType.isValid(type.asString())
        || data == null
        || !data.isObject()) {
      return Optional.empty();
    }
    return Optional.of(new EventRequest(type.asString(), data, sha256(body)));
  }

  String type() {
    return type;
  }

  /**
   * The SHA-256 of the body as it was received. A retry sends the same bytes. The same event
   * written in another way is another request.
   */
  byte[] hash() {
    return hash;
  }

  /** The body that every delivery of the event sends, with {@code created_at} in milliseconds. */
  byte[] render(UUID id, Instant createdAt) {
    ObjectNode body = JSON.createObjectNode();
    body.put("id", id.toString());
    body.put("type", type);
    body.put("created_at", MILLISECONDS.format(createdAt));
    body.set("data", data);
    return JSON.writeValueAsBytes(body);
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
