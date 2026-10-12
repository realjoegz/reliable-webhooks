package io.github.realjoegz.reliablewebhooks.api;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import org.apache.catalina.connector.Connector;
import org.springframework.boot.tomcat.TomcatConnectorCustomizer;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.RequestBodyAdviceAdapter;

/**
 * Refuses a request body above 256 KiB, on every route that reads one. The body is read here, up to
 * one byte past the limit, before a converter sees it. The check therefore does not depend on a
 * declared length.
 *
 * <p>This is the only place where the service reads a request body, so it never holds more of one
 * than the limit and one byte. Spring Boot and Tomcat would read a form or a multipart body in
 * three places ahead of this one, and know this limit in none of them. No route takes either, so
 * all three are off. The form-content filter and the multipart parsing of Spring are turned off in
 * application.yaml. For the third there is no property: Tomcat reads the body of a POST that is
 * sent as a form, up to 2 MB of it, when the parameters of the request are first asked for, and
 * Spring asks for them when a route is about to read the body of such a request, before it comes
 * here. This class therefore tells the connector of Tomcat to read no body for parameters.
 */
@ControllerAdvice
public class RequestBodyLimit extends RequestBodyAdviceAdapter
    implements TomcatConnectorCustomizer {

  static final int MAX_BYTES = 256 * 1024;

  @Override
  public boolean supports(
      MethodParameter parameter,
      Type targetType,
      Class<? extends HttpMessageConverter<?>> converterType) {
    return true;
  }

  @Override
  public HttpInputMessage beforeBodyRead(
      HttpInputMessage message,
      MethodParameter parameter,
      Type targetType,
      Class<? extends HttpMessageConverter<?>> converterType)
      throws IOException {
    byte[] body = message.getBody().readNBytes(MAX_BYTES + 1);
    if (body.length > MAX_BYTES) {
      throw new ResponseStatusException(
          HttpStatus.CONTENT_TOO_LARGE,
          "The request body must not exceed %d bytes".formatted(MAX_BYTES));
    }
    return new HttpInputMessage() {
      @Override
      public InputStream getBody() {
        return new ByteArrayInputStream(body);
      }

      @Override
      public HttpHeaders getHeaders() {
        return message.getHeaders();
      }
    };
  }

  @Override
  public void customize(Connector connector) {
    connector.setParseBodyMethods("");
  }
}
