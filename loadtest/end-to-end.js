// End to end: a steady 500 events a second for five minutes, spread over 100 endpoints of 20
// tenants. An event goes to one endpoint, so the replicas have 500 deliveries a second to make.
// How long a delivery took from its creation to its 2xx is read from the meter of the replicas
// and from the database, by loadtest/run.sh.

import exec from 'k6/execution';
import { createTenants, pick, replicas, sendEvent } from './lib/webhooks.js';

const TENANTS = 20;
const ENDPOINTS_EACH = 5;

export const options = {
  summaryTrendStats: ['med', 'p(90)', 'p(99)', 'max', 'count'],
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: 500,
      timeUnit: '1s',
      duration: '5m',
      preAllocatedVUs: 500,
    },
  },
  thresholds: {
    'http_req_failed{scenario:steady}': ['rate==0'],
    'dropped_iterations{scenario:steady}': ['count==0'],
  },
};

export function setup() {
  return createTenants(TENANTS, ENDPOINTS_EACH);
}

export default function (tenants) {
  const type = `load.${Math.floor(Math.random() * ENDPOINTS_EACH)}`;
  sendEvent(pick(replicas), pick(tenants), type, `steady-${exec.scenario.iterationInTest}`);
}
