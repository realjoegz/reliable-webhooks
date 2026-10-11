CREATE TABLE tenant (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name          text NOT NULL,
  api_key_hash  bytea NOT NULL UNIQUE,                 -- SHA-256 of the key
  status        text NOT NULL DEFAULT 'active' CHECK (status IN ('active','suspended')),
  rate_per_sec  double precision NOT NULL DEFAULT 50  CHECK (rate_per_sec >= 0),
  burst         integer NOT NULL DEFAULT 100          CHECK (burst >= 0),
  max_endpoints integer NOT NULL DEFAULT 20           CHECK (max_endpoints BETWEEN 0 AND 50),
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE rate_bucket (                              -- inserted with the tenant (tokens = burst);
  tenant_id   uuid PRIMARY KEY REFERENCES tenant,       -- written on every ingest, kept off the tenant row
  tokens      double precision NOT NULL,
  refilled_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE endpoint (                                 -- configuration; written only by the tenant
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenant,
  url         text NOT NULL,
  event_types text[] NOT NULL,                          -- exact names, or '*'
  secret      bytea NOT NULL,                           -- AES-256-GCM ciphertext
  prev_secret bytea,
  prev_secret_expires_at timestamptz,
  status      text NOT NULL DEFAULT 'active' CHECK (status IN ('active','deleted')),
  created_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz
);
CREATE INDEX endpoint_active ON endpoint (tenant_id) WHERE status = 'active';   -- ingest match, cap, list

CREATE TABLE endpoint_circuit (                         -- health; inserted with the endpoint,
  endpoint_id uuid PRIMARY KEY REFERENCES endpoint ON DELETE CASCADE,   -- afterwards written only by workers
  state       text NOT NULL DEFAULT 'closed' CHECK (state IN ('closed','open')),
  consecutive_failures integer NOT NULL DEFAULT 0,
  probe_failures       integer NOT NULL DEFAULT 0,
  opened_at     timestamptz,
  next_probe_at timestamptz
);
CREATE TABLE event (
  id              uuid PRIMARY KEY,
  tenant_id       uuid NOT NULL REFERENCES tenant,
  type            text NOT NULL,
  body            bytea NOT NULL,                       -- rendered once at ingest, sent byte-identical
  idempotency_key text NOT NULL,
  request_hash    bytea NOT NULL,
  trace_context   text,                                 -- this service's ingest span, not the caller's
  created_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX event_created ON event (created_at);       -- retention

CREATE TABLE delivery (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),   -- sent as webhook-id
  tenant_id   uuid NOT NULL REFERENCES tenant,
  event_id    uuid NOT NULL REFERENCES event,               -- RESTRICT: an event cannot vanish under a delivery
  endpoint_id uuid NOT NULL REFERENCES endpoint,
  state       text NOT NULL DEFAULT 'pending'
              CHECK (state IN ('pending','succeeded','dead','cancelled')),
  attempts    integer NOT NULL DEFAULT 0,                   -- claims in the current run
  replays     integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL,                     -- due time; while leased, the lease expiry
  lease_token uuid,                                         -- set by a claim, cleared by every other writer
  expires_at  timestamptz NOT NULL,                         -- created_at + 72 h, reset by replay
  dead_reason text,                                         -- exhausted | expired | endpoint_deleted
  created_at  timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  UNIQUE (event_id, endpoint_id)
);
CREATE INDEX delivery_head   ON delivery (endpoint_id, next_attempt_at) WHERE state = 'pending';  -- the claim
CREATE INDEX delivery_expiry ON delivery (expires_at)   WHERE state = 'pending';
CREATE INDEX delivery_done   ON delivery (completed_at) WHERE state <> 'pending';
CREATE INDEX delivery_by_endpoint ON delivery (endpoint_id, state, created_at, id);               -- list, bulk replay

CREATE TABLE delivery_attempt (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  delivery_id uuid NOT NULL REFERENCES delivery ON DELETE CASCADE,
  run         integer NOT NULL,                             -- delivery.replays at the time
  attempt_no  integer NOT NULL,
  probe       boolean NOT NULL DEFAULT false,
  outcome     text NOT NULL CHECK (outcome IN ('success','failure','unknown')),
  http_status integer,
  error       text,   -- timeout | connect | tls | deadline | response_too_large | destination_not_allowed | lease_expired
  remote_addr inet,
  started_at  timestamptz NOT NULL DEFAULT now(),
  duration_ms integer,
  replica     text,
  UNIQUE (delivery_id, run, attempt_no)
);
