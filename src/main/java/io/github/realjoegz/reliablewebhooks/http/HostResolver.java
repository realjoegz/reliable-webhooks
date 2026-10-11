package io.github.realjoegz.reliablewebhooks.http;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Turns a host into its addresses. A host is a name or an address literal, and a literal answers
 * with itself. The answer has at least one address.
 */
public interface HostResolver {

  InetAddress[] resolve(String host) throws UnknownHostException;
}
