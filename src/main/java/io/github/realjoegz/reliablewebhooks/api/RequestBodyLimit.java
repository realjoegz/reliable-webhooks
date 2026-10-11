package io.github.realjoegz.reliablewebhooks.api;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
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
 * declared length, and the service never holds more of a body than the limit and one byte.
 */
@ControllerAdvice
public class RequestBodyLimit extends RequestBodyAdviceAdapter {

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
}
