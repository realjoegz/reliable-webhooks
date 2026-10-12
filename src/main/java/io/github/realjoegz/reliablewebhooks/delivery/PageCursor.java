package io.github.realjoegz.reliablewebhooks.delivery;

import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Where a page of the delivery list ended: the creation time and the id of its last delivery. The
 * next page begins after that position in the order of the list.
 */
public record PageCursor(Instant createdAt, UUID id) {

  // A year of four digits keeps the time inside what a timestamptz column can hold, so a text
  // that no page handed out cannot fail the statement that it is given to.
  private static final Pattern POSITION =
      Pattern.compile(
          "(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,9})?Z)"
              + ",([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");

  /** The text that a page hands out and that the request for the next page sends back. */
  public String encode() {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString((createdAt + "," + id).getBytes(StandardCharsets.UTF_8));
  }

  /** Empty for a text that is not the encoding of a position. */
  public static Optional<PageCursor> parse(String text) {
    try {
      Matcher position =
          POSITION.matcher(new String(Base64.getUrlDecoder().decode(text), StandardCharsets.UTF_8));
      if (!position.matches()) {
        return Optional.empty();
      }
      return Optional.of(
          new PageCursor(Instant.parse(position.group(1)), UUID.fromString(position.group(2))));
    } catch (IllegalArgumentException | DateTimeException notACursor) {
      return Optional.empty();
    }
  }
}
