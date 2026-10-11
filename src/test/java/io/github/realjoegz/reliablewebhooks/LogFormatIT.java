package io.github.realjoegz.reliablewebhooks;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.realjoegz.reliablewebhooks.harness.InProcessReplica;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@InProcessReplica
@ExtendWith(OutputCaptureExtension.class)
class LogFormatIT {

  private static final Logger log = LoggerFactory.getLogger(LogFormatIT.class);

  @Autowired JsonMapper json;

  @Test
  void consoleLineIsAJsonObject(CapturedOutput output) {
    String message = "logged-by-the-test-" + UUID.randomUUID();

    log.info(message);

    String line =
        output
            .getOut()
            .lines()
            .filter(written -> written.contains(message))
            .findFirst()
            .orElseThrow();
    JsonNode entry = json.readTree(line);
    assertThat(entry.get("message").asString()).isEqualTo(message);
    assertThat(entry.get("level").asString()).isEqualTo("INFO");
  }
}
