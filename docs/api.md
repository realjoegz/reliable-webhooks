# API

Every route of a replica: who may call it, what it takes, what it returns and with which status. The reasons behind a route are in [design.md](design.md), and section numbers refer to it. A replica listens on port 8080 unless `server.port` says otherwise.

- [Conventions](#conventions)
- [Events](#events)
- [Endpoints](#endpoints)
- [Deliveries](#deliveries)
- [Admin routes](#admin-routes)
- [Health and metrics](#health-and-metrics)
- [What a subscriber receives](#what-a-subscriber-receives)

## Conventions

**Authentication.** You send a credential as `Authorization: Bearer <credential>`. The first segment of the path decides which one (section 11.1):

| Path | Credential |
|---|---|
| `/admin/...` | The admin token of the deployment |
| `/actuator/...` | None |
| Any other path | The API key of a tenant, `rwh_` and 43 more characters |

**Requests.** A body is JSON in UTF-8 without a byte order mark, at most 256 KiB, sent with `Content-Type: application/json`. Member names are in snake case. An id is a UUID.

**Responses.** A time is ISO 8601 in UTC, such as `2026-10-13T10:06:41.103319Z`. Zeros at the end of its fraction are left out three at a time, so a fraction has six digits, three or none. A member without a value is sent as `null`. Four responses carry an API key or a signing secret. Each is sent with `Cache-Control: no-store`, and no later response shows that value again.

**Errors.** An error is an RFC 9457 problem document, sent as `application/problem+json`:

```json
{
  "type": "rate_limited",
  "title": "Rate limited",
  "status": 429,
  "detail": "The rate limit of the tenant is used up.",
  "instance": "/v1/events"
}
```

Eight problems have a `type`, which is the name that the tables below give them. Every other error has none. Compare the `type` and the status, not the `detail`.

**Answers that any route can give.** The tables of the routes do not repeat them.

| Status | `type` | When |
|---|---|---|
| 400 | | An id in the path that is no UUID, a body that is not the JSON the route takes, or a parameter that is missing or invalid |
| 401 | | The credential is missing, malformed or unknown. Sent with `WWW-Authenticate: Bearer`, also for a path that has no route |
| 403 | `tenant_suspended` | Tenant routes: the tenant is suspended |
| 404 | | The path has no route, or the id does not exist. An id that belongs to another tenant gets the same answer |
| 405 | | The route does not take this method. `Allow` names the ones it takes |
| 413 | | The body is above 256 KiB |
| 415 | | The body is not sent as JSON, or its `Content-Type` names a charset other than UTF-8 |
| 429 | `concurrency_limit` | Tenant routes: the tenant already has 8 requests open on this replica. `Retry-After: 1` |
| 500 | | A failure that the service maps to nothing else. The document has no `detail` |
| 503 | `database_busy` | The database did not answer in time or its connection failed. `Retry-After: 1` |

A request is authenticated first, then counted against the 8 open requests of its tenant, and only then matched to a route.

## Events

### `POST /v1/events`

Accepts an event and creates one delivery for each active endpoint of the tenant that takes its type (section 6).

The body is `{"type": "order.created", "data": {"order": 1001}}`, and the header `Idempotency-Key` is required.

- `Idempotency-Key` has 1 to 255 printable ASCII characters.
- `type` has 1 to 128 characters of `[a-z0-9_.-]`.
- `data` is a JSON object. A number has at most 1,000 digits, a member name at most 1,024 bytes, and the body nests at most 500 levels deep.
- Members other than `type` and `data` are ignored.

| Status | `type` | When |
|---|---|---|
| 202 | | Accepted. The body is `{"id": "<event id>"}` |
| 202 | | The key and the body are those of an event that was accepted before. The answer has the id of that event and the header `Idempotent-Replay: true`. Nothing is stored. A repeat of an event that was already accepted is not charged. Of two requests with one key that run at the same time, both can be charged (section 12) |
| 400 | | The key is missing or invalid, or the body is not an event within the limits |
| 422 | `idempotency_key_reused` | The key belongs to an event that was sent with a different body. Bodies are compared byte for byte |
| 429 | `rate_limited` | The bucket of the tenant does not hold what the event costs. `Retry-After` is the number of seconds in which it refills that much |
| 429 | `rate_limit_unsatisfiable` | The rate of the tenant is 0, or the event costs more than its burst. No `Retry-After`, because waiting does not help |

An event costs one unit for each endpoint that it goes to, at least one, and one more for each full 4 KiB of its stored body (section 12).

### `GET /v1/events/{id}`

Returns an event with its deliveries, at most 50, in the order of their endpoint ids.

```json
{
  "id": "986fb1b2-1a5d-486d-a33a-887efbc51257",
  "type": "order.created",
  "created_at": "2026-10-13T10:06:41.274620Z",
  "deliveries": [
    {
      "id": "60f002ba-e54e-4172-8c49-ec9605474a7a",
      "endpoint_id": "48755163-1b47-4a46-a609-92eff66dc4e9",
      "state": "pending",
      "attempts": 1
    }
  ]
}
```

`state` is `pending`, `succeeded`, `dead` or `cancelled`. `attempts` counts the attempts of the current run of the delivery, as in the [delivery object](#deliveries). The answer is `200`, or `404` for an id that the tenant does not have.

## Endpoints

An endpoint is read as this object. The signing secret is not part of it.

```json
{
  "id": "48755163-1b47-4a46-a609-92eff66dc4e9",
  "url": "https://hooks.example.com/orders",
  "event_types": ["order.created"],
  "circuit": "closed",
  "created_at": "2026-10-13T10:06:41.103319Z"
}
```

`circuit` is `closed`, `open` or `half_open`. `half_open` is an open circuit whose next probe is due (section 5). Once an endpoint is deleted, every route of this section that takes its id answers `404`.

### `POST /v1/endpoints`

Registers an endpoint. The body is `{"url": "...", "event_types": ["order.created", "order.paid"]}`.

- `url` is an `https` URL of at most 2,048 characters, without userinfo, on port 443 or 1024 to 65535, whose host resolves to public addresses only (section 11.4).
- `event_types` has 1 to 20 entries. Each is an event type or `*`, which takes every type.

| Status | `type` | When |
|---|---|---|
| 201 | | Registered. The body is `{"id": "...", "secret": "whsec_..."}` |
| 400 | | `url` is missing, or `event_types` is missing or invalid |
| 409 | | The tenant already has as many active endpoints as its cap allows |
| 422 | `endpoint_url_not_allowed` | The URL breaks a rule above. The answer does not say which, and it is the same for a host that does not resolve |
| 429 | `lookup_busy` | The replica had no room to look the host up. `Retry-After: 2` |

A registration is not idempotent, and a URL can be registered more than once. When a registration got no answer, read `GET /v1/endpoints` before you send it again: the endpoint may exist, with a secret that was shown to nobody. Delete that endpoint, or rotate its secret (limitation 37 of the design).

### `GET /v1/endpoints`

Returns the active endpoints of the tenant as a JSON array of the object above, in the order of their creation. There are at most 50.

### `GET /v1/endpoints/{id}`

Returns one endpoint: `200`, or `404`.

### `PATCH /v1/endpoints/{id}`

Replaces the event types. The body is `{"event_types": [...]}`. The answer is `204`, `400` for an invalid list, or `404`. The URL of an endpoint cannot be changed: register a new endpoint for a new URL.

### `DELETE /v1/endpoints/{id}`

Deletes the endpoint: `204`, or `404`. Nothing is claimed for it from then on. A job that runs every 30 seconds makes its pending deliveries `cancelled` (section 15), and a cancelled delivery cannot be replayed.

### `POST /v1/endpoints/{id}/secret/rotate`

Gives the endpoint a new signing secret. It takes no body. The answer is `200` with `{"secret": "whsec_..."}`, or `404`. For 24 hours every request is signed with both secrets (section 11.3). Only one old secret is kept, so a second rotation within those 24 hours ends the first overlap.

### `POST /v1/endpoints/{id}/replay`

Replays dead deliveries of the endpoint, the oldest first (section 8). The body is `{"limit": 100}`. A call replays at most `limit` deliveries, at most 500, and at most as many as the burst of the tenant.

| Status | `type` | When |
|---|---|---|
| 200 | | The body is `{"replayed": 100, "remaining": true}`. Call again until `remaining` is `false` |
| 400 | | `limit` is missing, below 1 or not a whole number |
| 404 | | The tenant has no active endpoint with this id |
| 429 | `rate_limited`, `rate_limit_unsatisfiable` | The bucket does not hold one unit for each delivery of the call. A call that finds nothing dead costs nothing |

## Deliveries

A delivery is read as this object:

```json
{
  "id": "cfe598e8-dc06-40c0-9a9b-ae20d0316bb0",
  "event_id": "65d88920-ca53-49ce-81f5-f94a58933f8a",
  "endpoint_id": "5bdda398-a3a2-4b33-a457-3dda12f6980b",
  "state": "succeeded",
  "attempts": 2,
  "replays": 0,
  "dead_reason": null,
  "created_at": "2026-10-13T10:05:52.371151Z",
  "completed_at": "2026-10-13T10:05:57.994779Z"
}
```

- `id` is what a subscriber receives as `webhook-id`.
- `attempts` counts the attempts of the current run, the one in progress included. An attempt is counted when its delivery is claimed (section 7). A thirteenth claim sends nothing and makes the delivery `dead`, so such a delivery reads 13 and has twelve attempts in its log.
- `replays` is the number of the current run. A delivery that was never replayed is in run 0.
- `dead_reason` is `exhausted` or `expired` for a dead delivery and `endpoint_deleted` for a cancelled one.
- `completed_at` is set once the delivery is no longer pending.

### `GET /v1/deliveries`

Lists the deliveries of one endpoint in one state, ordered by creation time and id.

| Parameter | Value |
|---|---|
| `endpoint_id` | Required. A deleted endpoint can still be named |
| `state` | Required: `pending`, `succeeded`, `dead` or `cancelled` |
| `limit` | 1 to 100, and 100 when it is absent |
| `cursor` | The `next` of the page before |

The answer is `200` with `{"deliveries": [...], "next": "..."}`. `next` is `null` on the last page. The answer is `400` when one of the two required parameters is missing or a `state`, `limit` or `cursor` is invalid, and `404` for an endpoint that the tenant does not have.

### `GET /v1/deliveries/{id}`

Returns the delivery object with two more members after its own: `run`, and the attempt log of that run as `attempt_log`. Without a parameter it is the current run. `?run=0` asks for an earlier one.

```json
{
  "id": "cfe598e8-dc06-40c0-9a9b-ae20d0316bb0",
  "event_id": "65d88920-ca53-49ce-81f5-f94a58933f8a",
  "endpoint_id": "5bdda398-a3a2-4b33-a457-3dda12f6980b",
  "state": "succeeded",
  "attempts": 2,
  "replays": 0,
  "dead_reason": null,
  "created_at": "2026-10-13T10:05:52.371151Z",
  "completed_at": "2026-10-13T10:05:57.994779Z",
  "run": 0,
  "attempt_log": [
    {
      "attempt_no": 1,
      "outcome": "failure",
      "http_status": 503,
      "error": null,
      "started_at": "2026-10-13T10:05:52.568870Z",
      "duration_ms": 2
    },
    {
      "attempt_no": 2,
      "outcome": "success",
      "http_status": 200,
      "error": null,
      "started_at": "2026-10-13T10:05:57.930779Z",
      "duration_ms": 64
    }
  ]
}
```

- `outcome` is `success`, `failure` or `unknown`.
- An attempt that got a response has its `http_status` and no `error`. An attempt that got none has an `error` and no status: `timeout`, `connect`, `tls`, `deadline`, `response_too_large` or `destination_not_allowed` (section 7).
- An `unknown` attempt has the error `lease_expired` and no duration. The replica that made it recorded nothing, so the request may or may not have arrived.

The answer is `200`, or `404` for an id that the tenant does not have and for a run that the delivery does not have.

### `POST /v1/deliveries/{id}/replay`

Makes a dead delivery pending again in a new run. It takes no body.

| Status | `type` | When |
|---|---|---|
| 204 | | Replayed. The delivery is due within the next 60 seconds |
| 404 | | The tenant has no such delivery, or its endpoint is deleted |
| 409 | | The delivery is not dead |
| 429 | `rate_limited`, `rate_limit_unsatisfiable` | The bucket does not hold the one unit that a replay costs |

The unit is charged first and is not given back when the answer is `404` or `409`.

## Admin routes

These take the admin token and answer `401` without it. They are not counted against the open requests of a tenant.

### `POST /admin/tenants`

Creates a tenant. The body is `{"name": "..."}` with 1 to 200 characters. The answer is `201` with `{"id": "...", "api_key": "rwh_..."}`, or `400`. A new tenant may send 50 units a second with a burst of 100 and register 20 endpoints. The call is not idempotent: if its answer is lost, the tenant may exist with a key that nobody holds, and a repeat creates a second tenant (limitation 37 of the design).

### `PATCH /admin/tenants/{id}`

Changes the limits or the status of a tenant. The body has at least one of these members, and a member that is absent stays as it is:

| Member | Value |
|---|---|
| `rate_per_sec` | 0, or 0.000001 to 1,000,000 |
| `burst` | A whole number, 0 or more |
| `max_endpoints` | A whole number, 0 to 50 |
| `status` | `active` or `suspended` |

The answer is `204`, `400` for a body without any of the four or with a value out of range, or `404`.

A suspended tenant gets `403` on every tenant route, and no claim takes its deliveries until it is active again (section 5). The rate limit, the endpoint cap and the stop of deliveries hold on every replica from the next statement on. A replica caches a tenant with its key for 30 seconds (section 11.1), so a replica that did not handle the call can accept the key of a suspended tenant for that long.

### `POST /admin/tenants/{id}/api-key/rotate`

Replaces the API key of a tenant. It takes no body. The answer is `200` with `{"api_key": "rwh_..."}`, or `404`. The old key stops working at once on the replica that handled the call, and within 30 seconds on the others.

## Health and metrics

These take no credential, so keep them from the public at the ingress.

| Route | Answer |
|---|---|
| `GET /actuator` | `200` with links to the routes below |
| `GET /actuator/health` | The health of the replica as a whole: `200` with the status `UP`, or `503`. It answers `503` with `DOWN` while the liveness route below does |
| `GET /actuator/health/liveness` | `200` with `{"status": "UP"}`, or `503` with `DOWN` once the poller has not finished an iteration for 60 seconds |
| `GET /actuator/health/readiness` | `200` with `{"status": "UP"}`, or `503` with `OUT_OF_SERVICE` while the replica shuts down |
| `GET /actuator/prometheus` | `200` with the meters of section 10 in the Prometheus text format |

## What a subscriber receives

Each attempt is one `POST` to the URL of the endpoint.

| Header | Value |
|---|---|
| `Content-Type` | `application/json` |
| `User-Agent` | `reliable-webhooks` |
| `webhook-id` | The id of the delivery, the same on every attempt and after a replay |
| `webhook-timestamp` | The time at which this attempt was signed, in Unix seconds, on the clock of the database |
| `webhook-signature` | `v1,` and the signature in base64. For 24 hours after a rotation there are two, separated by a space, the one of the new secret first |
| `traceparent` | The trace context of the span of this attempt (section 10) |

The body is the event as it was stored when it was accepted, byte for byte the same on every attempt:

```json
{"id":"65d88920-ca53-49ce-81f5-f94a58933f8a","type":"order.created","created_at":"2026-10-13T10:05:52.368Z","data":{"order":1002,"total":"12.00"}}
```

What your answer means:

- Any 2xx status is a success. The body of your answer does not count, and it is never stored.
- Anything else is a failure and is tried again on the retry schedule (section 8): any other status, a redirect, which is not followed, and an answer that has not arrived after 15 seconds.
- A `Retry-After` in seconds on a failure can lengthen the wait for the next attempt to at most one hour. It cannot shorten it.
- You can get the same delivery more than once. Deduplicate on `webhook-id`.

### Verifying a request

Check every request with the signing secret that the registration of your endpoint returned. The signature is an HMAC-SHA256 over `{webhook-id}.{webhook-timestamp}.{body}`, in the format of Standard Webhooks, and the key is the secret without its `whsec_` prefix, decoded from base64. This is the reference verifier, which also refuses a request whose timestamp is more than 5 minutes away from the time of the check. `ReferenceVerifierTest` runs it against what the service signs and holds this copy to its source.

```java
public final class ReferenceVerifier {

  private static final Duration TOLERANCE = Duration.ofMinutes(5);

  private ReferenceVerifier() {}

  /**
   * Whether a request is genuine and recent.
   *
   * @param secret the signing secret of the endpoint, as its registration returned it
   * @param id the value of the webhook-id header
   * @param timestamp the value of the webhook-timestamp header
   * @param signatures the value of the webhook-signature header
   * @param body the body of the request, byte for byte as it arrived
   * @param now the current time
   */
  public static boolean isGenuine(
      String secret, String id, String timestamp, String signatures, byte[] body, Instant now)
      throws GeneralSecurityException {
    Instant signedAt;
    try {
      signedAt = Instant.ofEpochSecond(Long.parseLong(timestamp));
    } catch (NumberFormatException | DateTimeException notATime) {
      return false;
    }
    // A request that was recorded and is sent again later carries a valid signature. Its age is
    // what gives it away.
    if (Duration.between(signedAt, now).abs().compareTo(TOLERANCE) > 0) {
      return false;
    }

    byte[] key = Base64.getDecoder().decode(secret.substring("whsec_".length()));
    Mac mac = Mac.getInstance("HmacSHA256");
    mac.init(new SecretKeySpec(key, "HmacSHA256"));
    mac.update((id + "." + timestamp + ".").getBytes(StandardCharsets.UTF_8));
    byte[] expected = Base64.getEncoder().encode(mac.doFinal(body));

    // The header can carry several signatures, separated by spaces. One match is enough.
    for (String signature : signatures.split(" ")) {
      if (signature.startsWith("v1,")
          && MessageDigest.isEqual(
              expected, signature.substring(3).getBytes(StandardCharsets.UTF_8))) {
        return true;
      }
    }
    return false;
  }
}
```

[examples/subscriber/Subscriber.java](../examples/subscriber/Subscriber.java) is a subscriber in one file that carries this method.
