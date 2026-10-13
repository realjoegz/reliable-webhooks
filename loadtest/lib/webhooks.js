// What the scenarios share: the tenants of a run, and the request that sends an event.
//
// loadtest/compose.yaml gives the load generator its environment: REPLICAS, the addresses of the
// replicas, SINK, the address at which the replicas reach the subscriber of the load tests, and
// ADMIN_TOKEN.

import http from 'k6/http';

export const replicas = __ENV.REPLICAS.split(',');

// A rate limit that no scenario reaches: 100,000 units a second, and a bucket of as many.
const RATE_PER_SECOND = 100000;

// An event body of about 1 KiB, most of it this member.
const FILLER = 'x'.repeat(900);

export function pick(items) {
  return items[Math.floor(Math.random() * items.length)];
}

// One request of a setup or a teardown, to the first replica. Any answer but the expected one
// ends the run.
export function call(method, path, token, body, expected) {
  const response = http.request(method, replicas[0] + path, body && JSON.stringify(body), {
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
  });
  if (response.status !== expected) {
    throw new Error(`${method} ${path} answered ${response.status}: ${response.body}`);
  }
  return response;
}

// Gives the sink one of its two settings: the status that it answers a delivery with, or the
// milliseconds that it waits before it answers one.
export function tellSink(setting, value) {
  const response = http.put(`${__ENV.SINK}/${setting}`, String(value));
  if (response.status !== 204) {
    throw new Error(`The sink answered ${response.status} to ${setting} ${value}`);
  }
}

// Creates tenants with the raised rate limit, each with a number of endpoints at the sink.
// Endpoint n of a tenant takes the events of type load.n and of type load.all. An event of
// type load.n therefore goes to one endpoint, and one of type load.all to every endpoint of
// its tenant. Returns the API key and the endpoint ids of each tenant.
export function createTenants(count, endpointsEach) {
  const admin = __ENV.ADMIN_TOKEN;
  const tenants = [];
  for (let t = 0; t < count; t++) {
    const tenant = call('POST', '/admin/tenants', admin, { name: `load-${t}` }, 201).json();
    const limit = { rate_per_sec: RATE_PER_SECOND, burst: RATE_PER_SECOND };
    call('PATCH', `/admin/tenants/${tenant.id}`, admin, limit, 204);
    const endpoints = [];
    for (let n = 0; n < endpointsEach; n++) {
      const endpoint = { url: `${__ENV.SINK}/`, event_types: [`load.${n}`, 'load.all'] };
      endpoints.push(call('POST', '/v1/endpoints', tenant.api_key, endpoint, 201).json('id'));
    }
    tenants.push({ apiKey: tenant.api_key, endpoints });
  }
  return tenants;
}

// Sends one event of a tenant to a replica, under an idempotency key.
export function sendEvent(replica, tenant, type, key, timeout = '60s') {
  const event = { type, data: { key, filler: FILLER } };
  return http.post(`${replica}/v1/events`, JSON.stringify(event), {
    headers: {
      Authorization: `Bearer ${tenant.apiKey}`,
      'Content-Type': 'application/json',
      'Idempotency-Key': key,
    },
    timeout,
  });
}
