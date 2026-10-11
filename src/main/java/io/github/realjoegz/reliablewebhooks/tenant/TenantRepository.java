package io.github.realjoegz.reliablewebhooks.tenant;

import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

@Repository
public class TenantRepository {

  static final String INSERT_TENANT =
      "INSERT INTO tenant (name, api_key_hash) VALUES (:name, :hash) RETURNING id, burst";

  static final String INSERT_BUCKET =
      "INSERT INTO rate_bucket (tenant_id, tokens) VALUES (:id, :burst)";

  static final String FIND_BY_API_KEY_HASH =
      "SELECT id, status, rate_per_sec, burst FROM tenant WHERE api_key_hash = :hash";

  static final String REPLACE_API_KEY = "UPDATE tenant SET api_key_hash = :hash WHERE id = :id";

  private record Inserted(UUID id, int burst) {}

  private final JdbcClient jdbc;
  private final TransactionTemplate transactions;

  TenantRepository(
      @Qualifier("apiJdbc") JdbcClient jdbc,
      @Qualifier("apiTransactions") TransactionTemplate transactions) {
    this.jdbc = jdbc;
    this.transactions = transactions;
  }

  /** The tenant and its bucket commit together, so a tenant never exists without a bucket. */
  public UUID create(String name, byte[] apiKeyHash) {
    return transactions.execute(
        transaction -> {
          Inserted tenant =
              jdbc.sql(INSERT_TENANT)
                  .param("name", name)
                  .param("hash", apiKeyHash)
                  .query(
                      (row, n) ->
                          new Inserted(row.getObject("id", UUID.class), row.getInt("burst")))
                  .single();
          jdbc.sql(INSERT_BUCKET).param("id", tenant.id()).param("burst", tenant.burst()).update();
          return tenant.id();
        });
  }

  public Optional<Tenant> findByApiKeyHash(byte[] apiKeyHash) {
    return jdbc.sql(FIND_BY_API_KEY_HASH)
        .param("hash", apiKeyHash)
        .query(
            (row, n) ->
                new Tenant(
                    row.getObject("id", UUID.class),
                    row.getString("status"),
                    row.getDouble("rate_per_sec"),
                    row.getInt("burst")))
        .optional();
  }

  /** Returns false when no tenant has this id. */
  public boolean replaceApiKey(UUID id, byte[] apiKeyHash) {
    return jdbc.sql(REPLACE_API_KEY).param("id", id).param("hash", apiKeyHash).update() == 1;
  }
}
