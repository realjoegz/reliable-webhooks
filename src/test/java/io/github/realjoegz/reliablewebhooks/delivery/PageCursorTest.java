package io.github.realjoegz.reliablewebhooks.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class PageCursorTest {

  private static final String ID = "0b0e3f6a-5a0c-4d6e-9f43-1c2d3e4f5a6b";

  // A creation time has the microseconds of the database, and a position that lost them would
  // name another row or none.
  @ParameterizedTest
  @ValueSource(
      strings = {
        "2026-10-11T12:34:56.123456Z",
        "2026-10-11T12:34:56.120Z",
        "2026-10-11T12:34:56Z",
        "2026-10-11T12:00:00Z"
      })
  void encodedPositionIsParsedBackAsItWas(String createdAt) {
    PageCursor position = new PageCursor(Instant.parse(createdAt), UUID.fromString(ID));

    assertThat(PageCursor.parse(position.encode())).contains(position);
  }

  @Test
  void encodedPositionIsSafeInAQueryString() {
    PageCursor position =
        new PageCursor(Instant.parse("2026-10-11T12:34:56.123456Z"), UUID.fromString(ID));

    assertThat(position.encode()).matches("[A-Za-z0-9_-]+");
  }

  // The year of five digits is a time that Instant reads and that a timestamptz column cannot
  // hold.
  static Stream<String> textsThatAreNoPosition() {
    return Stream.of(
        "",
        "not base64 !",
        encoded("neither a time nor an id"),
        encoded("2026-10-11T12:34:56Z"),
        encoded("2026-10-11T12:34:56Z," + ID + ",more"),
        encoded("2026-13-11T12:34:56Z," + ID),
        encoded("+10000-01-01T00:00:00Z," + ID),
        encoded("2026-10-11T12:34:56Z,not-an-id"));
  }

  @ParameterizedTest
  @MethodSource("textsThatAreNoPosition")
  void textThatIsNoPositionIsRefused(String text) {
    assertThat(PageCursor.parse(text)).isEmpty();
  }

  private static String encoded(String position) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(position.getBytes(StandardCharsets.UTF_8));
  }
}
