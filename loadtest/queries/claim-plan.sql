-- The deliveries that are pending, and when the statistics of the delivery table were last
-- gathered: by autovacuum, and by the ANALYZE of loadtest/run.sh before the scenario.
SELECT (SELECT count(*) FROM delivery WHERE state = 'pending') AS pending,
       to_char(last_autoanalyze, 'HH24:MI:SS') AS last_autoanalyze,
       to_char(last_analyze, 'HH24:MI:SS') AS last_analyze,
       to_char(now(), 'HH24:MI:SS') AS now
FROM pg_stat_user_tables
WHERE relname = 'delivery';

-- The claim of DeliveryRepository with its parameters written in: no endpoint and no tenant is
-- full, 64 attempts are free and the lease is 60 s. EXPLAIN runs the statement, and the
-- transaction is rolled back, so the claim leases nothing.
BEGIN;
EXPLAIN (ANALYZE)
WITH eligible AS MATERIALIZED (
  SELECT e.id AS endpoint_id, e.tenant_id, c.state AS circuit
  FROM endpoint e
  JOIN tenant t           ON t.id = e.tenant_id
  JOIN endpoint_circuit c ON c.endpoint_id = e.id
  WHERE e.status = 'active'
    AND t.status = 'active'
    AND (c.state = 'closed' OR c.next_probe_at <= now())
    AND e.id        <> ALL ('{}'::uuid[])
    AND e.tenant_id <> ALL ('{}'::uuid[])
), head AS (
  SELECT g.endpoint_id, g.tenant_id, g.circuit, h.id, h.next_attempt_at, h.abandoned
  FROM eligible g
  CROSS JOIN LATERAL (
    SELECT d.id, d.next_attempt_at, d.lease_token IS NOT NULL AS abandoned
    FROM delivery d
    WHERE d.endpoint_id = g.endpoint_id
      AND d.state = 'pending'
      AND d.next_attempt_at <= now()
    ORDER BY d.next_attempt_at
    LIMIT 1
    FOR UPDATE OF d SKIP LOCKED
  ) h
), ranked AS (
  SELECT *, row_number() OVER (PARTITION BY tenant_id ORDER BY next_attempt_at) AS nth_for_tenant
  FROM head
), picked AS (
  SELECT * FROM ranked
  WHERE nth_for_tenant <= 4
  ORDER BY nth_for_tenant, next_attempt_at
  LIMIT 64
)
UPDATE delivery d
SET attempts = d.attempts + 1,
    lease_token = gen_random_uuid(),
    next_attempt_at = now() + interval '60 seconds'
FROM picked p
WHERE d.id = p.id
RETURNING d.id, d.tenant_id, d.event_id, d.endpoint_id, d.attempts, d.replays,
          d.lease_token, p.abandoned, p.circuit;
ROLLBACK;
