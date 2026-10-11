package io.github.realjoegz.reliablewebhooks.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.JsonNode;

class EventRequestTest {

  private static final UUID ID = UUID.fromString("0190a8f4-5d7e-7c3a-9b1f-2e4d6a8c0b12");
  private static final Instant CREATED_AT = Instant.parse("2026-03-04T05:06:07.089Z");
  private static final String RENDERED_UP_TO_DATA =
      """
      {"id":"0190a8f4-5d7e-7c3a-9b1f-2e4d6a8c0b12","type":"%s",\
      "created_at":"2026-03-04T05:06:07.089Z","data":""";

  @Test
  void bodyIsRenderedAsIdTypeCreatedAtAndData() {
    EventRequest request =
        parsed(
            """
            {"type":"order.created","data":{"order":17}}""");

    assertThat(request.type()).isEqualTo("order.created");
    assertThat(rendered(request))
        .isEqualTo(RENDERED_UP_TO_DATA.formatted("order.created") + "{\"order\":17}}");
  }

  // Instant.toString() prints the first instant without a fraction and the second with one
  // digit. The third would round up to .121.
  @Test
  void createdAtIsRenderedWithThreeFractionDigits() {
    EventRequest request = parsed("{\"type\":\"a\",\"data\":{}}");

    assertThat(renderedAt(request, "2026-03-04T05:06:07Z"))
        .contains("\"created_at\":\"2026-03-04T05:06:07.000Z\"");
    assertThat(renderedAt(request, "2026-03-04T05:06:07.100Z"))
        .contains("\"created_at\":\"2026-03-04T05:06:07.100Z\"");
    assertThat(renderedAt(request, "2026-03-04T05:06:07.120999999Z"))
        .contains("\"created_at\":\"2026-03-04T05:06:07.120Z\"");
  }

  // Each of these numbers changes when it is read into a double: the first loses its last
  // digits, the second its trailing zero, the third becomes infinity and the fourth is rounded.
  @Test
  void numbersOfTheDataKeepTheirExactValue() {
    String data =
        """
        {"precise":0.1000000000000000000000001,"price":12.50,"huge":1E+400,\
        "count":12345678901234567890123.0}""";

    assertThat(rendered(parsed(withData(data))))
        .isEqualTo(RENDERED_UP_TO_DATA.formatted("a") + data + "}");
  }

  @Test
  void numberMayBeRenderedInAnotherNotationOfTheSameValue() {
    EventRequest request =
        parsed(
            """
            {"type":"a","data":{"hundred":1e2,"fifteen":1.50e1}}""");

    assertThat(rendered(request))
        .isEqualTo(RENDERED_UP_TO_DATA.formatted("a") + "{\"hundred\":1E+2,\"fifteen\":15.0}}");
  }

  // The sign, the point and the E are not digits, and the digits of the exponent are. The last
  // number has 1, 995 and 4 of them.
  static Stream<Arguments> numbersOfOneThousandDigits() {
    return Stream.of(
        arguments(named("integer", "7".repeat(1000))),
        arguments(named("negative integer", "-" + "7".repeat(1000))),
        arguments(named("fraction", "0." + "7".repeat(999))),
        arguments(named("exponent", "-7." + "7".repeat(995) + "E+1234")));
  }

  @ParameterizedTest
  @MethodSource("numbersOfOneThousandDigits")
  void numberOfOneThousandDigitsKeepsItsExactValue(String number) {
    String data = "{\"n\":" + number + "}";

    assertThat(rendered(parsed(withData(data))))
        .isEqualTo(RENDERED_UP_TO_DATA.formatted("a") + data + "}");
  }

  @Test
  void numberWithTheLargestExponentOfABigDecimalKeepsItsExactValue() {
    String data =
        """
        {"large":1E+2147483647,"small":1E-2147483647}""";

    assertThat(rendered(parsed(withData(data))))
        .isEqualTo(RENDERED_UP_TO_DATA.formatted("a") + data + "}");
  }

  // A name of 512 two-byte characters shows that the bytes are counted and not the characters.
  static Stream<Arguments> memberNamesOfOneThousandTwentyFourBytes() {
    return Stream.of(
        arguments(named("one byte for each character", "k".repeat(1024))),
        arguments(named("two bytes for each character", "é".repeat(512))));
  }

  @ParameterizedTest
  @MethodSource("memberNamesOfOneThousandTwentyFourBytes")
  void memberNameOfOneThousandTwentyFourBytesIsRendered(String name) {
    String data = "{\"" + name + "\":1}";

    assertThat(rendered(parsed(withData(data))))
        .isEqualTo(RENDERED_UP_TO_DATA.formatted("a") + data + "}");
  }

  // The parser answers a name that is in its table with the String it made when it first read
  // that name, so a name that comes back as another String has left the table. The library
  // empties the table once it holds more than 6,000 names. That number cannot be set, so the
  // test holds the library to it.
  @Test
  void memberNameIsKeptAfterItsBodyAndDroppedOnceMoreThanSixThousandAreKept()
      throws ReflectiveOperationException {
    String body = withData("{\"" + "k".repeat(1024) + "\":1}");
    String kept = nameInTheDataOf(parsed(body));

    assertThat(nameInTheDataOf(parsed(body))).isSameAs(kept);

    for (int i = 0; i <= 6000; i++) {
      parsed(withData("{\"n" + i + "\":1}"));
    }

    assertThat(nameInTheDataOf(parsed(body))).isNotSameAs(kept);
  }

  // The body is the first level and the data the second.
  @Test
  void bodyOfFiveHundredLevelsIsRendered() {
    String data = nested(499);

    assertThat(rendered(parsed(withData(data))))
        .isEqualTo(RENDERED_UP_TO_DATA.formatted("a") + data + "}");
  }

  static Stream<Arguments> bodiesOutsideTheLimits() {
    return Stream.of(
        arguments(named("integer of 1,001 digits", withData("{\"n\":" + "7".repeat(1001) + "}"))),
        arguments(
            named("fraction of 1,001 digits", withData("{\"n\":0." + "7".repeat(1000) + "}"))),
        arguments(
            named(
                "1,001 digits with those of the exponent",
                withData("{\"n\":7." + "7".repeat(996) + "E+1234}"))),
        arguments(named("exponent of eleven digits", withData("{\"n\":1e99999999999}"))),
        arguments(named("exponent above a BigDecimal", withData("{\"n\":1E+2147483648}"))),
        arguments(named("exponent below a BigDecimal", withData("{\"n\":1E-2147483648}"))),
        arguments(named("scale outside a BigDecimal", withData("{\"n\":1.5E-2147483647}"))),
        arguments(
            named("member name of 1,025 bytes", withData("{\"" + "k".repeat(1025) + "\":1}"))),
        arguments(
            named(
                "member name of 513 characters in 1,025 bytes",
                withData("{\"k" + "é".repeat(512) + "\":1}"))),
        arguments(
            named(
                "member name of 1,025 bytes outside the data",
                "{\"type\":\"a\",\"data\":{},\"" + "k".repeat(1025) + "\":1}")),
        arguments(named("501 levels of objects", withData(nested(500)))),
        arguments(
            named(
                "501 levels, arrays among them",
                withData("{\"a\":" + "[".repeat(499) + "]".repeat(499) + "}"))),
        arguments(
            named(
                "number of 1,001 digits outside the data",
                "{\"type\":\"a\",\"data\":{},\"extra\":" + "7".repeat(1001) + "}")));
  }

  @ParameterizedTest
  @MethodSource("bodiesOutsideTheLimits")
  void bodyOutsideTheLimitsDoesNotParse(String body) {
    assertThat(EventRequest.parse(bytes(body))).isEmpty();
  }

  @Test
  void lastOfTheMembersWithTheSameNameCounts() {
    EventRequest request =
        parsed(
            """
            {"type":"a","type":"b","data":{"k":1,"z":0,"k":2}}""");

    assertThat(request.type()).isEqualTo("b");
    assertThat(rendered(request))
        .isEqualTo(RENDERED_UP_TO_DATA.formatted("b") + "{\"k\":2,\"z\":0}}");
  }

  @Test
  void dataKeepsTheOrderOfItsMembersAndItsNestedValues() {
    String data =
        """
        {"z":[1,"two",null,true,{"deep":[]}],"a":{"b":{"c":"café \\" \\\\"}}}""";

    assertThat(rendered(parsed(withData(data))))
        .isEqualTo(RENDERED_UP_TO_DATA.formatted("a") + data + "}");
  }

  @Test
  void whitespaceAndMembersOtherThanTypeAndDataAreNotRendered() {
    EventRequest request =
        parsed(
            """
            { "data" : { "order" : 17 },
              "extra" : [1], "type" : "order.paid" }
            """);

    assertThat(rendered(request))
        .isEqualTo(RENDERED_UP_TO_DATA.formatted("order.paid") + "{\"order\":17}}");
  }

  @Test
  void hashIsTheSha256OfTheBodyAsReceived() throws NoSuchAlgorithmException {
    byte[] body = bytes("{\"type\":\"a\",\"data\":{}}");
    byte[] sameEventWrittenAnotherWay = bytes("{\"type\":\"a\", \"data\":{}}");

    assertThat(EventRequest.parse(body).orElseThrow().hash())
        .isEqualTo(MessageDigest.getInstance("SHA-256").digest(body))
        .isNotEqualTo(EventRequest.parse(sameEventWrittenAnotherWay).orElseThrow().hash());
  }

  static Stream<String> bodiesThatAreNotAnEvent() {
    return Stream.of(
        "",
        "{",
        "not json",
        "null",
        "\"order.created\"",
        "[{\"type\":\"a\",\"data\":{}}]",
        "{\"type\":\"a\",\"data\":{}} trailing",
        "{\"type\":\"a\",\"data\":{}}{}",
        "{\"type\":\"a\",\"data\":{\"n\":NaN}}",
        "{\"data\":{}}",
        "{\"type\":null,\"data\":{}}",
        "{\"type\":17,\"data\":{}}",
        "{\"type\":[\"a\"],\"data\":{}}",
        "{\"type\":\"\",\"data\":{}}",
        "{\"type\":\"Order.Created\",\"data\":{}}",
        "{\"type\":\"" + "a".repeat(129) + "\",\"data\":{}}",
        "{\"type\":\"a\"}",
        "{\"type\":\"a\",\"data\":null}",
        "{\"type\":\"a\",\"data\":[]}",
        "{\"type\":\"a\",\"data\":\"text\"}",
        "{\"type\":\"a\",\"data\":17}");
  }

  @ParameterizedTest
  @MethodSource("bodiesThatAreNotAnEvent")
  void bodyThatIsNotAnObjectWithAnEventTypeAndAnObjectAsDataDoesNotParse(String body) {
    assertThat(EventRequest.parse(bytes(body))).isEmpty();
  }

  @Test
  void typeOfOneHundredTwentyEightCharactersParses() {
    String type = "a".repeat(128);

    assertThat(parsed("{\"type\":\"" + type + "\",\"data\":{}}").type()).isEqualTo(type);
  }

  private static EventRequest parsed(String body) {
    return EventRequest.parse(bytes(body)).orElseThrow();
  }

  private static String withData(String data) {
    return "{\"type\":\"a\",\"data\":" + data + "}";
  }

  /** A JSON object that holds an object that holds an object, down to the given level. */
  private static String nested(int levels) {
    return "{\"a\":".repeat(levels - 1) + "{}" + "}".repeat(levels - 1);
  }

  // The request has no accessor for its data, because nothing but its rendering reads it.
  private static String nameInTheDataOf(EventRequest request) throws ReflectiveOperationException {
    Field data = EventRequest.class.getDeclaredField("data");
    data.setAccessible(true);
    return ((JsonNode) data.get(request)).propertyNames().iterator().next();
  }

  private static String rendered(EventRequest request) {
    return new String(request.render(ID, CREATED_AT), StandardCharsets.UTF_8);
  }

  private static String renderedAt(EventRequest request, String createdAt) {
    return new String(request.render(ID, Instant.parse(createdAt)), StandardCharsets.UTF_8);
  }

  private static byte[] bytes(String body) {
    return body.getBytes(StandardCharsets.UTF_8);
  }
}
