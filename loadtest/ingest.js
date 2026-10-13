// Ingest: the rate of POST /v1/events goes up in steps, and the summary shows for each step the
// 99th percentile of the time that a request took, the share of requests that failed and the
// requests that the load generator could not start in time.
//
// A step holds its rate for two minutes. The events go to 20 tenants with one endpoint each, so
// every event has one delivery, which the replicas make while the next events arrive. A step
// passes when its 99th percentile is under the bound and it had no error: every request was
// answered with a 202, and the load generator started every request when it was due. A step has
// a sender for each half second of its rate, so a request is dropped only when the replicas
// have answered nothing for that long.

import exec from 'k6/execution';
import { createTenants, pick, replicas, sendEvent } from './lib/webhooks.js';

const RATES = [250, 500, 750, 1000, 1250, 1500];
const STEP_SECONDS = 120;
const P99_BOUND_MS = 100;

const scenarios = {};
const thresholds = {};
RATES.forEach((rate, step) => {
  const name = `rate_${String(rate).padStart(4, '0')}`;
  scenarios[name] = {
    executor: 'constant-arrival-rate',
    rate,
    timeUnit: '1s',
    duration: `${STEP_SECONDS}s`,
    startTime: `${step * STEP_SECONDS}s`,
    preAllocatedVUs: rate / 2,
  };
  thresholds[`http_req_duration{scenario:${name}}`] = [`p(99)<${P99_BOUND_MS}`];
  thresholds[`http_req_failed{scenario:${name}}`] = ['rate==0'];
  thresholds[`dropped_iterations{scenario:${name}}`] = ['count==0'];
});

export const options = {
  summaryTrendStats: ['med', 'p(90)', 'p(99)', 'max', 'count'],
  scenarios,
  thresholds,
};

export function setup() {
  return createTenants(20, 1);
}

export default function (tenants) {
  const key = `${exec.scenario.name}-${exec.scenario.iterationInTest}`;
  sendEvent(pick(replicas), pick(tenants), 'load.0', key);
}
