package io.github.realjoegz.reliablewebhooks.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class AddressPolicyTest {

  private final AddressPolicy policy = new AddressPolicy();

  @ParameterizedTest
  @CsvSource({
    "0.0.0.0/8,       0.0.0.0,      0.255.255.255",
    "10.0.0.0/8,      10.0.0.0,     10.255.255.255",
    "100.64.0.0/10,   100.64.0.0,   100.127.255.255",
    "127.0.0.0/8,     127.0.0.0,    127.255.255.255",
    "169.254.0.0/16,  169.254.0.0,  169.254.255.255",
    "172.16.0.0/12,   172.16.0.0,   172.31.255.255",
    "192.0.0.0/24,    192.0.0.0,    192.0.0.255",
    "192.0.2.0/24,    192.0.2.0,    192.0.2.255",
    "192.88.99.0/24,  192.88.99.0,  192.88.99.255",
    "192.168.0.0/16,  192.168.0.0,  192.168.255.255",
    "198.18.0.0/15,   198.18.0.0,   198.19.255.255",
    "198.51.100.0/24, 198.51.100.0, 198.51.100.255",
    "203.0.113.0/24,  203.0.113.0,  203.0.113.255",
    "224.0.0.0/4,     224.0.0.0,    239.255.255.255",
    "240.0.0.0/4,     240.0.0.0,    255.255.255.255"
  })
  void ipv4DenyRangeIsDeniedFromItsFirstAddressToItsLast(String range, String first, String last)
      throws UnknownHostException {
    assertThat(policy.allows(address(first))).as("first of %s", range).isFalse();
    assertThat(policy.allows(address(last))).as("last of %s", range).isFalse();
    assertThat(policy.allows(ipv4Mapped(first))).as("first of %s, IPv4-mapped", range).isFalse();
    assertThat(policy.allows(ipv4Mapped(last))).as("last of %s, IPv4-mapped", range).isFalse();
  }

  // The address before and the address after each deny range, where that is not another range.
  @ParameterizedTest
  @ValueSource(
      strings = {
        "1.0.0.0",
        "9.255.255.255",
        "11.0.0.0",
        "100.63.255.255",
        "100.128.0.0",
        "126.255.255.255",
        "128.0.0.0",
        "169.253.255.255",
        "169.255.0.0",
        "172.15.255.255",
        "172.32.0.0",
        "191.255.255.255",
        "192.0.1.0",
        "192.0.1.255",
        "192.0.3.0",
        "192.88.98.255",
        "192.88.100.0",
        "192.167.255.255",
        "192.169.0.0",
        "198.17.255.255",
        "198.20.0.0",
        "198.51.99.255",
        "198.51.101.0",
        "203.0.112.255",
        "203.0.114.0",
        "223.255.255.255"
      })
  void ipv4AddressNextToADenyRangeIsAllowed(String neighbour) throws UnknownHostException {
    assertThat(policy.allows(address(neighbour))).isTrue();
    assertThat(policy.allows(ipv4Mapped(neighbour))).isTrue();
  }

  @ParameterizedTest
  @CsvSource({
    "2001::/23,     2001::,     2001:1ff:ffff:ffff:ffff:ffff:ffff:ffff",
    "2001:db8::/32, 2001:db8::, 2001:db8:ffff:ffff:ffff:ffff:ffff:ffff",
    "2002::/16,     2002::,     2002:ffff:ffff:ffff:ffff:ffff:ffff:ffff"
  })
  void ipv6DenyRangeIsDeniedFromItsFirstAddressToItsLast(String range, String first, String last)
      throws UnknownHostException {
    assertThat(policy.allows(address(first))).as("first of %s", range).isFalse();
    assertThat(policy.allows(address(last))).as("last of %s", range).isFalse();
  }

  // The two ends of 2000::/3 and the address before and after each deny range inside it.
  @ParameterizedTest
  @ValueSource(
      strings = {
        "2000::",
        "3fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        "2000:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        "2001:200::",
        "2001:db7:ffff:ffff:ffff:ffff:ffff:ffff",
        "2001:db9::",
        "2001:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        "2003::",
        "2606:4700:4700::1111"
      })
  void globalUnicastIpv6OutsideTheDenyRangesIsAllowed(String allowed) throws UnknownHostException {
    assertThat(policy.allows(address(allowed))).isTrue();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "::",
        "::1",
        "::7f00:1",
        "64:ff9b::7f00:1",
        "100::1",
        "1fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        "4000::",
        "fc00::1",
        "fd00:ec2::254",
        "fe80::1",
        "fec0::1",
        "ff02::1"
      })
  void ipv6OutsideGlobalUnicastIsDenied(String denied) throws UnknownHostException {
    assertThat(policy.allows(address(denied))).isFalse();
  }

  // The JDK hands out an Inet4Address for a mapped literal, so the policy would never see the
  // sixteen bytes. This test builds them.
  @Test
  void ipv4MappedAddressIsAnInet6AddressWhenThePolicyJudgesIt() throws UnknownHostException {
    InetAddress mapped = ipv4Mapped("127.0.0.1");

    assertThat(mapped).isInstanceOf(Inet6Address.class);
    assertThat(mapped.getAddress()).hasSize(16);
    assertThat(policy.allows(mapped)).isFalse();
  }

  private static InetAddress address(String literal) throws UnknownHostException {
    return InetAddress.getByName(literal);
  }

  private static InetAddress ipv4Mapped(String ipv4) throws UnknownHostException {
    byte[] mapped = new byte[16];
    mapped[10] = (byte) 0xff;
    mapped[11] = (byte) 0xff;
    System.arraycopy(address(ipv4).getAddress(), 0, mapped, 12, 4);
    return Inet6Address.getByAddress(null, mapped, -1);
  }
}
