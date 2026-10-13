// Kill under load: 300 events a second for two minutes, to 10 tenants with three endpoints each.
// An event goes to every endpoint of its tenant, so the replicas have 900 deliveries a second
// to make. loadtest/run.sh kills the second replica with SIGKILL once a third of the events are
// in, and reads from the database and from the sink what the kill left.
//
// In this scenario the sink waits 50 ms before it answers a delivery, as a subscriber that does
// some work would. About 45 requests are then open at any moment, and the kill finds the
// replica with requests that it has sent and not recorded.
//
// A sender is a producer with the addresses of both replicas. It sends an event to the replica
// that answered it last and gives it a second to answer. When no answer comes, it sends the same
// request under the same idempotency key to the other replica. When the answer refuses the
// event for now, with a 429 or a 503, it waits the second that such an answer names and sends
// the request to the same replica again. It sends an event five times at most, and the event
// is accepted once a 202 has answered it.

import { sleep } from 'k6';
import exec from 'k6/execution';
import { Counter } from 'k6/metrics';
import { createTenants, pick, replicas, sendEvent, tellSink } from './lib/webhooks.js';

const accepted = new Counter('events_accepted');
// A 202 that names an event which an earlier request with the same key had stored.
const acceptedAsRepeat = new Counter('events_accepted_as_repeat');
const sentAgain = new Counter('requests_sent_again');
const givenUp = new Counter('events_given_up');

const SENDS_PER_EVENT = 5;

// The replica that this sender uses. Every virtual user has its own.
let current = Math.floor(Math.random() * replicas.length);

export const options = {
  summaryTrendStats: ['med', 'p(90)', 'p(99)', 'max', 'count'],
  scenarios: {
    kill: {
      executor: 'constant-arrival-rate',
      rate: 300,
      timeUnit: '1s',
      duration: '2m',
      preAllocatedVUs: 400,
    },
  },
  thresholds: {
    events_given_up: ['count==0'],
    'dropped_iterations{scenario:kill}': ['count==0'],
  },
};

export function setup() {
  tellSink('delay', 50);
  return createTenants(10, 3);
}

export default function (tenants) {
  const tenant = pick(tenants);
  const key = `kill-${exec.scenario.iterationInTest}`;
  for (let sends = 1; sends <= SENDS_PER_EVENT; sends++) {
    if (sends > 1) {
      sentAgain.add(1);
    }
    const response = sendEvent(replicas[current], tenant, 'load.all', key, '1s');
    if (response.status === 202) {
      accepted.add(1);
      if (response.headers['Idempotent-Replay'] === 'true') {
        acceptedAsRepeat.add(1);
      }
      return;
    }
    if (response.status === 0) {
      current = (current + 1) % replicas.length;
    } else {
      sleep(1);
    }
  }
  givenUp.add(1);
}
