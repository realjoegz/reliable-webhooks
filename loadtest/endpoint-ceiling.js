// Per-endpoint ceiling: one endpoint with a backlog of 50,000 deliveries, which two replicas
// drain while they have nothing else to do. loadtest/run.sh reads from the database how many
// deliveries a second each replica made, and reads what a claim cost while they did.
//
// The backlog is built behind an open circuit. The sink answers 503 while the events arrive.
// The first attempts fail, the fifth failure opens the circuit of the endpoint, and the
// deliveries that follow are held. Once the last event is in, the sink answers 200 again. The
// next probe succeeds and closes the circuit, and the backlog is drained.
//
// Twelve senders send the events as fast as they can. A sender keeps to one replica, six to
// each, because a tenant may have eight requests open on a replica.

import exec from 'k6/execution';
import { createTenants, replicas, sendEvent, tellSink } from './lib/webhooks.js';

export const options = {
  summaryTrendStats: ['med', 'p(90)', 'p(99)', 'max', 'count'],
  scenarios: {
    backlog: {
      executor: 'shared-iterations',
      vus: 12,
      iterations: 50000,
      maxDuration: '5m',
    },
  },
  thresholds: {
    'http_req_failed{scenario:backlog}': ['rate==0'],
  },
};

export function setup() {
  tellSink('status', 503);
  return createTenants(1, 1);
}

export default function (tenants) {
  const replica = replicas[exec.vu.idInTest % replicas.length];
  sendEvent(replica, tenants[0], 'load.0', `backlog-${exec.scenario.iterationInTest}`);
}

export function teardown() {
  tellSink('status', 200);
}
