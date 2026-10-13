# Documents

| Document | What it is |
|---|---|
| [design.md](design.md) | The design: the guarantees, the data model, every statement that matters with its SQL, security, the limitations, operations, the test strategy and the alternatives that were rejected |
| [failure-table.md](failure-table.md) | The six process kills and the 31 failures, each with the tests that prove what happens and how those tests force it. The build checks it |
| [behaviour.md](behaviour.md) | Where each area is specified and proven, what proves each guarantee, and what the tests show beyond the design. The build checks every test that it names |
| [api.md](api.md) | Every route with its authentication, its request, its response and its status codes, what a subscriber receives and how it verifies a request |
| [deploy.md](deploy.md) | The Helm chart, its values and probes, how a replica stops, how migrations behave in a rollout, the smoke test on kind and the Terraform module for AWS |
| [performance.md](performance.md) | Four load tests on one laptop: what each run does, what it measured and what limits each number |

Where to start:

- To judge the design, read sections 1, 6, 7 and 14 of [design.md](design.md): the guarantees, ingest, the delivery worker and the limitations.
- To check a claim, find its row in [failure-table.md](failure-table.md) and run the test that it links.
- To call the service, read [api.md](api.md).
- To run it on a cluster, read [deploy.md](deploy.md).
