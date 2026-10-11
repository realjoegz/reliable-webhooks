package io.github.realjoegz.reliablewebhooks.http;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.springframework.stereotype.Component;

/** The resolver of the operating system. */
@Component
public class SystemHostResolver implements HostResolver {

  @Override
  public InetAddress[] resolve(String host) throws UnknownHostException {
    return InetAddress.getAllByName(host);
  }
}
