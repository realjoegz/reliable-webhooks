// What a run begins with, before its scenario: events for 20 tenants with one endpoint each, at
// a rate that rises to 1,000 a second in 30 seconds and stays there for four and a half
// minutes. loadtest/run.sh runs it and records nothing of it.
//
// A replica that has just started runs its code interpreted. A database that has just been
// created has one WAL segment. It creates every further one, 16 MB at a time, and commits wait
// while it does, until it has the segments of two checkpoints and uses them again. With
// max_wal_size at 1 GB those are about 64, and the events of the warm-up write more WAL than
// that. After it the replicas have compiled what they run, and the database has the WAL
// segments of one that has been running.

import { sleep } from 'k6';
import exec from 'k6/execution';
import { call, createTenants, pick, replicas, sendEvent } from './lib/webhooks.js';

export const options = {
  teardownTimeout: '3m',
  scenarios: {
    warm_up: {
      executor: 'ramping-arrival-rate',
      startRate: 100,
      timeUnit: '1s',
      stages: [
        { target: 1000, duration: '30s' },
        { target: 1000, duration: '4m30s' },
      ],
      preAllocatedVUs: 300,
    },
  },
};

export function setup() {
  return createTenants(20, 1);
}

export default function (tenants) {
  sendEvent(pick(replicas), pick(tenants), 'load.0', `warm-up-${exec.scenario.iterationInTest}`);
}

// Waits until every delivery of the warm-up is made, and then deletes its endpoints: a claim
// looks at every active endpoint, and those of the warm-up are no part of the scenario.
export function teardown(tenants) {
  for (const tenant of tenants) {
    for (const endpoint of tenant.endpoints) {
      const pending = `/v1/deliveries?endpoint_id=${endpoint}&state=pending&limit=1`;
      let waited = 0;
      while (call('GET', pending, tenant.apiKey, null, 200).json('deliveries').length > 0) {
        if (waited++ === 120) {
          throw new Error(`Endpoint ${endpoint} has a pending delivery after 120 s`);
        }
        sleep(1);
      }
      call('DELETE', `/v1/endpoints/${endpoint}`, tenant.apiKey, null, 204);
    }
  }
}
