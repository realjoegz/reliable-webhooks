package io.github.realjoegz.reliablewebhooks.harness;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.resttestclient.autoconfigure.RestTestClientBuilderCustomizer;
import org.springframework.boot.test.http.server.LocalTestWebServer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * The client that a test calls its replica with. It is the one that Spring Boot would configure,
 * except that it sends its requests with the HTTP client of the JDK. Left to itself, Spring picks
 * Apache HttpClient once the service has it on the class path, with the defaults of that library:
 * it sends a request again when the answer is a 429 or a 503 with {@code Retry-After}. A test has
 * to see the first answer.
 */
@Configuration(proxyBeanMethods = false)
class ReplicaClient {

  @Bean
  RestTestClient restTestClient(
      ApplicationContext replica, ObjectProvider<RestTestClientBuilderCustomizer> customizers) {
    RestTestClient.Builder<?> builder =
        RestTestClient.bindToServer(new JdkClientHttpRequestFactory())
            .uriBuilderFactory(LocalTestWebServer.obtain(replica).uriBuilderFactory());
    customizers.orderedStream().forEach(customizer -> customizer.customize(builder));
    return builder.build();
  }
}
