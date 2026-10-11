package io.github.realjoegz.reliablewebhooks.http;

import io.github.realjoegz.reliablewebhooks.WebhooksProperties;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Decides whether the service may connect to an address. The ranges are written out, because the
 * JDK predicates such as {@code isSiteLocalAddress} leave out shared address space, the
 * documentation ranges and most of what IPv6 reserves.
 *
 * <p>A policy can be given ranges that it lets through although it would deny them. They exist for
 * tests and docker compose, where the receiver has a private address.
 */
@Component
public class AddressPolicy {

  private static final List<Cidr> IPV4_DENIED =
      cidrs(
          "0.0.0.0/8",
          "10.0.0.0/8",
          "100.64.0.0/10",
          "127.0.0.0/8",
          "169.254.0.0/16",
          "172.16.0.0/12",
          "192.0.0.0/24",
          "192.0.2.0/24",
          "192.88.99.0/24",
          "192.168.0.0/16",
          "198.18.0.0/15",
          "198.51.100.0/24",
          "203.0.113.0/24",
          "224.0.0.0/4",
          "240.0.0.0/4");

  // Everything outside global unicast is denied without being listed: ::1, fc00::/7, fe80::/10,
  // 64:ff9b::/96 and multicast among it.
  private static final Cidr IPV6_GLOBAL_UNICAST = Cidr.parse("2000::/3");

  private static final List<Cidr> IPV6_DENIED = cidrs("2001::/23", "2001:db8::/32", "2002::/16");

  private static final int IPV4_LENGTH = 4;
  private static final int IPV4_MAPPED_PREFIX_LENGTH = 12;

  private final List<Cidr> allowedAnyway;

  @Autowired
  public AddressPolicy(WebhooksProperties properties, MeterRegistry meters) {
    this(properties.ssrf().allowCidrs());
    // The Prometheus registry renders this name as webhooks_ssrf_allowlist_entries.
    meters.gauge("webhooks.ssrf.allowlist.entries", allowedAnyway, List::size);
  }

  AddressPolicy(List<String> allowedCidrs) {
    this.allowedAnyway = allowedCidrs.stream().map(Cidr::parse).toList();
  }

  public boolean allows(InetAddress address) {
    byte[] bytes = address.getAddress();
    if (bytes.length == IPV4_LENGTH) {
      return allowsIpv4(bytes);
    }
    if (isIpv4Mapped(bytes)) {
      return allowsIpv4(Arrays.copyOfRange(bytes, IPV4_MAPPED_PREFIX_LENGTH, bytes.length));
    }
    return isAllowedAnyway(bytes)
        || (IPV6_GLOBAL_UNICAST.contains(bytes)
            && IPV6_DENIED.stream().noneMatch(range -> range.contains(bytes)));
  }

  private boolean allowsIpv4(byte[] bytes) {
    return isAllowedAnyway(bytes) || IPV4_DENIED.stream().noneMatch(range -> range.contains(bytes));
  }

  private boolean isAllowedAnyway(byte[] bytes) {
    return allowedAnyway.stream().anyMatch(range -> range.contains(bytes));
  }

  // ::ffff:a.b.c.d reaches the IPv4 host a.b.c.d, so it is judged as that host.
  private static boolean isIpv4Mapped(byte[] bytes) {
    for (int i = 0; i < 10; i++) {
      if (bytes[i] != 0) {
        return false;
      }
    }
    return bytes[10] == (byte) 0xff && bytes[11] == (byte) 0xff;
  }

  private static List<Cidr> cidrs(String... notations) {
    return Stream.of(notations).map(Cidr::parse).toList();
  }

  private record Cidr(byte[] network, int prefixLength) {

    // A range can come from the configuration. Only a dotted quad, or what starts like an IPv6
    // literal and has a colon, is handed to InetAddress. It parses those without a lookup, and
    // would look anything else up as a name.
    private static final Pattern NOTATION =
        Pattern.compile(
            "(?<address>((25[0-5]|2[0-4]\\d|1?\\d?\\d)\\.){3}(25[0-5]|2[0-4]\\d|1?\\d?\\d)"
                + "|(?=[^/]*:)[0-9A-Fa-f:][0-9A-Fa-f:.]*)/(?<prefix>\\d{1,3})");

    static Cidr parse(String notation) {
      Matcher parts = NOTATION.matcher(notation);
      if (!parts.matches()) {
        throw new IllegalArgumentException("Not an address range in CIDR notation: " + notation);
      }
      byte[] network;
      try {
        network = InetAddress.getByName(parts.group("address")).getAddress();
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException("Not an address range in CIDR notation: " + notation, e);
      }
      int prefixLength = Integer.parseInt(parts.group("prefix"));
      if (prefixLength > network.length * 8) {
        throw new IllegalArgumentException("Prefix too long for its address: " + notation);
      }
      return new Cidr(network, prefixLength);
    }

    boolean contains(byte[] address) {
      if (address.length != network.length) {
        return false;
      }
      int wholeBytes = prefixLength / 8;
      for (int i = 0; i < wholeBytes; i++) {
        if (address[i] != network[i]) {
          return false;
        }
      }
      int remainingBits = prefixLength % 8;
      if (remainingBits == 0) {
        return true;
      }
      int mask = 0xff << (8 - remainingBits);
      return (address[wholeBytes] & mask) == (network[wholeBytes] & mask);
    }
  }
}
