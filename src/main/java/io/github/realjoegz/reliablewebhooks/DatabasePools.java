package io.github.realjoegz.reliablewebhooks;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Database;
import io.github.realjoegz.reliablewebhooks.WebhooksProperties.Pool;
import java.time.Duration;
import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

@Configuration(proxyBeanMethods = false)
public class DatabasePools {

  /**
   * How long a pool may test a connection before handing it out. HikariCP looks at the borrow wait
   * again only after that test, so a borrow that meets a dead connection can end this much later
   * than its wait. The HikariCP default is 5 s, which is longer than the wait itself.
   */
  public static final Duration ALIVENESS_TEST_TIMEOUT = Duration.ofSeconds(1);

  @Bean
  HikariDataSource apiDataSource(WebhooksProperties properties) {
    return pool("api", properties.database(), properties.database().api());
  }

  @Bean
  HikariDataSource workerDataSource(WebhooksProperties properties) {
    return pool("worker", properties.database(), properties.database().worker());
  }

  @Bean
  JdbcClient apiJdbc(@Qualifier("apiDataSource") DataSource api) {
    return JdbcClient.create(api);
  }

  @Bean
  TransactionTemplate apiTransactions(@Qualifier("apiDataSource") DataSource api) {
    return new TransactionTemplate(new JdbcTransactionManager(api));
  }

  @Bean
  JdbcClient workerJdbc(@Qualifier("workerDataSource") DataSource worker) {
    return JdbcClient.create(worker);
  }

  @Bean
  TransactionTemplate workerTransactions(@Qualifier("workerDataSource") DataSource worker) {
    return new TransactionTemplate(new JdbcTransactionManager(worker));
  }

  private static HikariDataSource pool(String name, Database database, Pool bounds) {
    Assert.hasText(database.url(), "webhooks.database.url must be set");

    PGSimpleDataSource postgres = new PGSimpleDataSource();
    postgres.setUrl(database.url());
    postgres.setUser(database.username());
    postgres.setPassword(database.password());
    // Set after the URL, so a parameter in the URL cannot remove a time bound.
    postgres.setConnectTimeout((int) bounds.connectTimeout().toSeconds());
    postgres.setSocketTimeout((int) bounds.socketTimeout().toSeconds());
    postgres.setTcpKeepAlive(bounds.tcpKeepAlive());
    // Startup options are the session defaults, so RESET cannot remove them either.
    postgres.setOptions(
        "-c lock_timeout=%d -c statement_timeout=%d -c idle_in_transaction_session_timeout=%d"
            .formatted(
                bounds.lockTimeout().toMillis(),
                bounds.statementTimeout().toMillis(),
                bounds.idleInTransactionTimeout().toMillis()));

    HikariConfig pool = new HikariConfig();
    pool.setPoolName(name);
    pool.setDataSource(postgres);
    pool.setMaximumPoolSize(bounds.maxSize());
    pool.setConnectionTimeout(bounds.borrowTimeout().toMillis());
    pool.setValidationTimeout(ALIVENESS_TEST_TIMEOUT.toMillis());
    return new HikariDataSource(pool);
  }
}
