package io.github.realjoegz.reliablewebhooks.http;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.springframework.stereotype.Component;

/**
 * Decides whether the service may connect to an address. The ranges are written out, because the
 * JDK predicates such as {@code isSiteLocalAddress} leave out shared address space, the
 * documentation ranges and most of what IPv6 reserves.
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

  public boolean allows(InetAddress address) {
    byte[] bytes = address.getAddress();
    if (bytes.length == IPV4_LENGTH) {
      return allowsIpv4(bytes);
    }
    if (isIpv4Mapped(bytes)) {
      return allowsIpv4(Arrays.copyOfRange(bytes, IPV4_MAPPED_PREFIX_LENGTH, bytes.length));
    }
    return IPV6_GLOBAL_UNICAST.contains(bytes)
        && IPV6_DENIED.stream().noneMatch(range -> range.contains(bytes));
  }

  private static boolean allowsIpv4(byte[] bytes) {
    return IPV4_DENIED.stream().noneMatch(range -> range.contains(bytes));
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

    static Cidr parse(String notation) {
      int slash = notation.indexOf('/');
      try {
        // An address literal is parsed, not looked up.
        byte[] network = InetAddress.getByName(notation.substring(0, slash)).getAddress();
        return new Cidr(network, Integer.parseInt(notation.substring(slash + 1)));
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException(notation, e);
      }
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
