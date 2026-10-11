package io.github.realjoegz.reliablewebhooks.api;

import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.springframework.boot.jackson.autoconfigure.JsonFactoryBuilderCustomizer;
import org.springframework.core.MethodParameter;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.RequestBodyAdviceAdapter;
import tools.jackson.core.TokenStreamFactory;
import tools.jackson.core.json.JsonFactoryBuilder;

/**
 * Has every request body read as UTF-8, the one encoding that RFC 8259 allows, on every route that
 * reads one. The JSON library has a second parser, for characters, which counts a member name by
 * its characters and not by its bytes and keeps the names in a table of its own. A body reaches
 * that parser in two ways, and this class closes both. The JSON converter of Spring decodes a body
 * itself when the Content-Type names a charset that is not a Unicode one, so a request that names
 * any charset but UTF-8 is refused. The library detects UTF-16 and UTF-32 by the first bytes of a
 * body, so the JSON mapper of the API has that detection turned off.
 *
 * <p>The refusal needs the header only, so it comes before every other advice on a body. {@link
 * RequestBodyLimit} then reads no body that is refused here, and a request that breaks both rules
 * answers 415. For the customizer the order changes nothing: no other one touches the detection.
 */
@ControllerAdvice
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RequestBodyCharset extends RequestBodyAdviceAdapter
    implements JsonFactoryBuilderCustomizer {

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
      Class<? extends HttpMessageConverter<?>> converterType) {
    MediaType contentType = message.getHeaders().getContentType();
    Charset declared = contentType == null ? null : contentType.getCharset();
    if (declared != null && !declared.equals(StandardCharsets.UTF_8)) {
      throw new ResponseStatusException(
          HttpStatus.UNSUPPORTED_MEDIA_TYPE, "The request body must be in UTF-8");
    }
    return message;
  }

  @Override
  public void customize(JsonFactoryBuilder factory) {
    factory.disable(TokenStreamFactory.Feature.CHARSET_DETECTION);
  }
}
