package io.github.realjoegz.reliablewebhooks.ingest;

import java.util.regex.Pattern;

/** The form of an event type: 1 to 128 characters of {@code [a-z0-9_.-]}. */
public final class EventType {

  private static final Pattern FORM = Pattern.compile("[a-z0-9_.-]{1,128}");

  private EventType() {}

  public static boolean isValid(String candidate) {
    return FORM.matcher(candidate).matches();
  }
}
