# Deploying on Kubernetes

The chart in `deploy/helm/reliable-webhooks` installs the replicas of the service: a Deployment, a Service, a ServiceAccount and a PodDisruptionBudget, and on request a HorizontalPodAutoscaler and a ServiceMonitor. It installs no database, no Secret and no ingress.

CI installs the chart with Helm 4.3 on a kind cluster of Kubernetes 1.37 and runs the API through its Service ([The smoke test](#the-smoke-test)). It has been installed on no other version and on no other kind of cluster. Section numbers refer to [design.md](design.md).

## What the chart needs

**An image.** No image of the service is published. Build it from the `Dockerfile` of the repository and push it to a registry that the cluster pulls from:

```
docker build --tag registry.example.com/reliable-webhooks:1 .
docker push registry.example.com/reliable-webhooks:1
```

**A PostgreSQL database.** The tests run against PostgreSQL 18. The chart is given a JDBC URL and a user name, and a deployment that is meant to last points them at a managed database. [A cluster and a database on AWS](#a-cluster-and-a-database-on-aws) describes one. The PostgreSQL in `deploy/kind` is for the smoke test only: one pod whose data is gone with it.

- The user owns the schema. The first replica that starts creates the tables ([Database migrations](#database-migrations)).
- A replica refuses to start unless the database runs at `read committed` and does not have `synchronous_commit` off (section 15).
- A replica holds 32 connections, 16 in each of its two pools, and Flyway opens its own while a replica starts. In a rollout an old replica keeps its connections until it has drained, and the rollout does not wait for that before it starts the next new replica. The database can therefore see twice the replicas, and needs room for 32 connections times twice their largest number, and a few on top.
- The URL can carry parameters of the PostgreSQL driver. It cannot remove a time bound of the pools (section 12), and the chart refuses a URL with a `password` parameter, because the password comes from the Secret.
- The URL decides whether the connection is encrypted (limitation 38). The service sets nothing about TLS, so the driver, pgjdbc 42.7, does what its documentation says:
  - Without `sslmode` it asks the server for TLS and goes on without it when the server offers none.
  - With `sslmode=require` it refuses such a server. It does not check the certificate of the server.
  - With `sslmode=verify-full` it checks the certificate and the host name against a root certificate that it reads from a file. The image has no such file, and the chart has no value that mounts one.
  - No test connects to a database over TLS. The databases of the tests, of `compose.yaml` and of the smoke test offer none, which is why their URLs carry no `sslmode`.

**A Secret** in the namespace of the release, with three keys:

| Key | What it is |
|---|---|
| `database-password` | The password of the database user |
| `admin-token` | The token of the `/admin` routes, at least 32 characters |
| `secret-encryption-key` | The key that the signing secrets are encrypted with in the database: 32 bytes in base64 |

```
kubectl create namespace webhooks
kubectl --namespace webhooks create secret generic reliable-webhooks-credentials \
  --from-literal=database-password='the password of the database user' \
  --from-literal=admin-token="$(openssl rand -hex 32)" \
  --from-literal=secret-encryption-key="$(openssl rand -base64 32)"
```

- The chart reads none of the three from values, and `extraEnv` cannot name their variables. `scripts/check-chart.sh` holds the chart to both.
- A replica reads the Secret when it starts. A changed Secret reaches the replicas with the next rollout, which `kubectl rollout restart` starts.
- The service cannot change the encryption key of a database that is in use (limitation 18). A replica with another key cannot read the signing secrets that are stored, so the key has to be kept for as long as the database is.

## Installing

```
helm install reliable-webhooks deploy/helm/reliable-webhooks \
  --namespace webhooks \
  --set image.repository=registry.example.com/reliable-webhooks \
  --set-string image.tag=1 \
  --set 'database.url=jdbc:postgresql://db.example.com:5432/webhooks?sslmode=require' \
  --set database.username=webhooks \
  --set secret.name=reliable-webhooks-credentials
```

The chart does not render until these five are set, and `scripts/check-chart.sh` renders it with these five and nothing else. The URL is in quotes because a shell reads its `?` as a pattern. The tag is given with `--set-string` because it is a number here: `--set` would hand the chart a number, and the chart takes a tag as a string only. The notes that Helm prints after the install say how to wait for the replicas and how to create the first tenant. `helm upgrade` with the same values and another `image.tag` rolls a new image out.

## Values

| Value | Default | What it is for |
|---|---|---|
| `image.repository`, `image.tag` | none | The image of the service. A tag that is a number is given with `--set-string`, or in quotes in a file of values. |
| `image.pullPolicy` | `IfNotPresent` | |
| `database.url` | none | The JDBC URL of the database, which starts with `jdbc:postgresql://`. |
| `database.username` | none | The database user. |
| `secret.name` | none | The Secret above. |
| `replicaCount` | `2` | The number of replicas, unless `autoscaling.enabled` is true. With two, the API is answered and deliveries are sent while one of them is gone. |
| `extraEnv` | `[]` | Further environment variables of a replica. Every setting of the service can be given this way, as Spring Boot reads a setting from the environment. [Tracing](#tracing) is an example. |
| `resources` | 500m and 1Gi requested, 2 CPUs and 1Gi as the limit | A starting point and not a measurement: the load tests of [performance.md](performance.md) ran on one laptop, with replicas that had no limit of their own. Memory is requested at its limit, so a replica is not placed on a node that cannot give it that much. The image sets no heap size, so the JVM takes its default, a quarter of the memory limit, as the most its heap may grow to. That is 256 MiB of the default limit, and no load test ran a replica with that heap. |
| `startupProbe.periodSeconds`, `startupProbe.failureThreshold` | `2`, `150` | How long a replica may take to start: the two multiplied, which is 5 minutes. |
| `preStopSleepSeconds` | `5` | How long a replica that is being removed goes on as before, ahead of its SIGTERM. |
| `terminationGracePeriodSeconds` | `35` | How long a replica has from the beginning of its removal until it is killed. The chart does not render unless this leaves 30 s after the sleep. |
| `service.type`, `service.port`, `service.nodePort` | `ClusterIP`, `8080`, none | The Service of the API, of the type `ClusterIP` or `NodePort`. A node port is the whole port of the API on every node, with `/admin` and `/actuator` ([What the chart leaves to the cluster](#what-the-chart-leaves-to-the-cluster)). |
| `podDisruptionBudget.maxUnavailable` | `1` | How many replicas a drain of nodes may take away at a time. |
| `autoscaling.enabled` | `false` | [Scaling](#scaling). |
| `autoscaling.minReplicas`, `.maxReplicas`, `.targetCPUUtilizationPercentage` | `2`, `4`, `70` | |
| `serviceMonitor.enabled`, `serviceMonitor.labels` | `false`, `{}` | A ServiceMonitor for a cluster with the Prometheus Operator, with the labels that its Prometheus selects one by. It names `/actuator/prometheus` on the port of the API, which takes no key (section 10). |

`values.schema.json` checks the values before anything is rendered: a value that the chart does not know is refused, and so is a value of the wrong type or outside its range.

Not values, because nothing about them depends on the cluster:

- **The security context.** A replica runs as the user 65532 of the image and not as root, with a read-only root filesystem, without privilege escalation, with every capability dropped and with the default seccomp profile of the runtime. `/tmp` is a volume of the pod, because Tomcat keeps a working directory there (section 15). The namespace of the smoke test enforces the `restricted` Pod Security Standard, so the API server would refuse a replica there that may run as root, may escalate its privileges, keeps a capability or has no seccomp profile. The standard does not ask for the read-only root filesystem. The smoke test reads it from the replicas that it has started.
- **No token for the API of Kubernetes.** The replicas have a ServiceAccount of their own, and its token is not mounted. The service does not call that API. The smoke test requires that nothing is mounted in a replica but `/tmp`.
- **The rollout.** One new replica is started at a time, and an old one is stopped only once the new one is Ready (`maxSurge: 1`, `maxUnavailable: 0`). A replica that fails a startup invariant (section 15), cannot reach the database or cannot migrate it never becomes Ready, so such a release stops at its first replica and the old replicas keep running. No test rolls a release out: the smoke test installs one release and upgrades none. The two settings are rendered and held against the schema of a Deployment, and no rollout has been run on a cluster.

## Probes

All three ask the health endpoint of the replica, which takes no key (section 11.1) and borrows no database connection (section 15).

| Probe | Asks | When it fails |
|---|---|---|
| Startup | `/actuator/health/liveness`, every 2 s | After 150 failures in a row the replica is restarted. Until the probe has passed once, the other two do not run. |
| Liveness | `/actuator/health/liveness`, every 10 s | After 3 failures in a row the replica is restarted. |
| Readiness | `/actuator/health/readiness`, every 10 s | After 3 failures in a row the replica is taken out of the Service until the probe passes again. |

- **Liveness is the poller.** The liveness group of the service holds the `poller` indicator, which is down when the loop of the poller has finished no iteration for 60 s (section 15). A replica in that state still answers requests and delivers nothing, and a restart is what helps. A database that is down does not fail the probe: a claim that fails ends its iteration like any other. Replicas are therefore not restarted during a database outage, which a restart would not end. `PollerHealthIT` shows a replica whose poller is stuck failing the liveness probe and passing the readiness probe, and `DeliveryPollerTest` that the loop goes on after a claim that failed.
- **Readiness is not the database either.** It is the readiness state of Spring Boot and nothing else. A replica is ready from the end of its start to its SIGTERM. During a database outage the replicas stay in the Service and answer `503 database_busy` themselves (section 5), and they serve again without a restart once the database is back (row 8 of the failure table).
- **The startup probe is there for the migrations.** The web server of a replica listens only after Flyway has migrated the database, so nothing answers a probe while a migration runs. Without the startup probe, the liveness probe would restart a replica in the middle of a long migration.

## Stopping a replica

Kubernetes removes a replica in a rollout, when it scales down, when a node is drained and when a pod is deleted. Each time:

1. The pod is marked as terminating, and Kubernetes begins to take it out of the Service. At the same moment the preStop hook starts: `preStopSleepSeconds` of sleep, during which the replica works as before. It answers the requests that still reach it, and it claims and sends deliveries. The hook is the sleep action of Kubernetes, which is on by default from Kubernetes 1.30, and the chart does not render for an earlier version (`kubeVersion`). It needs no binary in the image, which has no `sleep`.
2. The replica gets its SIGTERM and drains (section 15): the poller stops claiming, the attempts in flight finish and record their results, the web server finishes the requests that are open, and the process ends by itself with exit code 143. An attempt is cut at 15 s, the drain waits at most 20 s for the attempts, and the design asks for at least 30 s of grace. `kubectl` shows a pod whose container ended with 143 as `Error` for the moment before the pod is gone.
3. A replica that is still running when `terminationGracePeriodSeconds` are over is killed. The seconds count from step 1, so the sleep is part of them. The default of 35 is the sleep and the 30 s.

A replica that is killed in step 3 loses nothing. That is a SIGKILL during the drain, which is K4 or K5 of the kill table: the delivery that was in flight is sent again after its lease has run out, with the same `webhook-id`.

What is tested and what is not:

- `GracefulShutdownIT` (row 20 of the failure table) proves the drain of a replica as a process, and the job `local-stack` of the CI workflow requires the exit code 143 from a container of the image.
- The smoke test deletes a replica that has nothing in flight, and the cluster goes on as section [The smoke test](#the-smoke-test) says.
- No test sends requests while a replica is being removed. That the sleep keeps a request from reaching a replica that has stopped listening is what the hook is for, and nothing in this repository measures it. A request that is refused that way stores nothing, and the producer sends it again with the same `Idempotency-Key` (section 6).

## Database migrations

A replica migrates the database when it starts, with Flyway, before its pools open and before its web server listens. Nothing else runs migrations: there is no job and no hook of Helm. The repository has one migration, `V1__schema.sql`, so no test applies a second one to a database that is in use. What follows is how Flyway 12.4 behaves with the settings that the service leaves at their defaults, and what the smoke test shows of it.

**Several replicas start at once.** Flyway takes an advisory lock in PostgreSQL before it creates its history table and before it applies a migration, and holds it in a transaction until that step is done.

- The replica that gets the lock migrates. A replica that does not get it asks again once a second. When it has the lock, it reads the history again, finds the migrations applied and applies nothing.
- The smoke test starts two replicas together on an empty database. It requires that one of them logged the migration as applied, that the other logged `No migration necessary`, and that neither was restarted.
- A replica asks 50 times after its first try and then gives up, which ends its start. Kubernetes starts it again, and it waits again. A migration that holds the lock for longer than those 50 s therefore shows as restarts of the replicas that wait for it, and they start once it is done.
- The lock ends with its transaction. A replica that dies while it holds the lock leaves no lock behind.

**A rolling update.** The rollout starts one new replica while the old ones run, so the new release migrates the database under the replicas of the old one, and they go on using it until the rollout has replaced them.

- A migration therefore has to leave the old release working. It may add to the schema, and it must not remove or rename what the old release reads or writes. What a release has stopped using is removed by a later one.
- A migration is not cut off by the time bounds of the pools, because Flyway has connections of its own (section 2). The statements of the running replicas are. While a migration holds a lock on a table, a statement that needs the table waits for at most the lock timeout of its pool, which is 2 s for the API and 5 s for the worker (section 12). A request is then answered with `503 database_busy` (row 31 of the failure table), and a claim fails and is made again.
- A migration has the time of the startup probe. A replica that is still migrating when the 5 minutes are over is restarted, and its migration with it. A migration that needs longer is given a larger `startupProbe.failureThreshold` for its release.

**A rollback of the image.** `helm rollback` puts the image of the earlier release back and changes nothing in the database. Flyway does not undo a migration, and the service has no script that would.

- A replica of the earlier image finds a history with a migration that it does not have. Flyway logs that the schema is newer than the latest migration it knows, applies nothing and lets the replica start.
- The smoke test shows that start: it adds the row of a migration that the image does not have to the history, as a later release leaves it, and deletes a replica. The replica that the Deployment starts in its place logs the warning and becomes Ready. No `helm rollback` is run in it.
- Whether the earlier release then works is the question of the rule above. If every migration since left it working, a rollback of the image is safe and the schema stays ahead of it. If one did not, the way back is a new release that repairs the schema, or the backup of the database.

## Scaling

`replicaCount` sets the number of replicas. With `autoscaling.enabled` the chart leaves it to a HorizontalPodAutoscaler, which keeps the average CPU use of the replicas near `targetCPUUtilizationPercentage` of their request, between `minReplicas` and `maxReplicas`. It needs a metrics server in the cluster.

- The autoscaler is off by default because its target is not a measured number: the load tests of [performance.md](performance.md) ran two replicas on one laptop and none on a cluster. CPU is a signal that needs nothing but a metrics server. The age of the oldest due delivery (`webhooks_oldest_due_age_seconds`, section 10) says more about a backlog, and scaling on it takes a metrics adapter that the chart does not assume.
- Every replica is 32 more connections to the database.
- What the design bounds for each replica grows with their number: 4 requests in flight to one endpoint and 16 for one tenant (section 7), and 8 open requests of one tenant (section 12). The rate limit of a tenant does not grow. It is one bucket in the database for all replicas.
- A replica that the autoscaler removes is stopped like any other.

The HorizontalPodAutoscaler and the ServiceMonitor are rendered and held against their schemas by `scripts/check-chart.sh`. Neither has been installed on a cluster: the cluster of the smoke test has no metrics server and no Prometheus Operator.

## Tracing

A replica exports its spans once it is told where to (section 10). In the chart that is one entry of `extraEnv`:

```yaml
extraEnv:
  - name: MANAGEMENT_OPENTELEMETRY_TRACING_EXPORT_OTLP_ENDPOINT
    value: http://collector.observability:4318/v1/traces
```

A replica samples a tenth of its traces. A further entry, `MANAGEMENT_TRACING_SAMPLING_PROBABILITY`, changes that share. `compose.yaml` gives its replicas both entries, the second with `1.0`.

## What the chart leaves to the cluster

- **Publishing the API.** The chart has no Ingress. A replica serves plain HTTP on port 8080, and every credential of the API is a bearer token, so whatever publishes the Service also has to end TLS (limitation 38). `/actuator` is served on the port of the API, and the scrape and the health probes take no key. Whatever publishes the Service has to keep that path from the public (limitation 34), and it is also where unauthenticated floods are stopped (limitation 9). The `/admin` routes are for the operator and take one static token (limitation 18). Nothing needs them outside the cluster.
- **The network.** The chart has no NetworkPolicy. A replica needs the database, and the subscribers wherever they are. It refuses addresses that are not public by itself (section 11.4), and the `unsafe-local` profile that lifts this is for tests: a replica that is started without the profile and with one of its allowances does not start (section 15).
- **The database**, its backups and its failover (limitation 3).
- **Pulling the image.** The chart has no value for an image pull secret, and `values.schema.json` refuses one. The replicas run under a ServiceAccount of their own, so a pull secret of the default account of the namespace does not reach them either. The nodes therefore have to be able to pull the image by themselves. The image is named by its tag, and the chart has no value for a digest.
- **Placing the replicas.** The chart has no value for a node selector, for tolerations, for affinity or for topology spread constraints, and the Deployment sets none of them. Nothing in the chart keeps two replicas off the same node.

## The smoke test

```
scripts/check-chart.sh
scripts/kind-smoke.sh
```

The first needs `helm` 4, `kubeconform` and the network. The second needs `docker`, `kind`, `kubectl`, `helm`, `curl` 7.82 or later and `jq`. The job `helm-chart` of the CI workflow runs both, with Helm 4.3.0 and kubeconform 0.8.0.

`scripts/check-chart.sh` needs no cluster. It runs `helm lint` and renders the chart three times: with the values of the smoke test, with those and the autoscaler and the ServiceMonitor turned on, and with the five values of [Installing](#installing) and nothing else, which is the chart at its defaults with a `ClusterIP` Service. Each time `kubeconform` holds every manifest against the schema of its kind for Kubernetes 1.37, and the kinds have to be the four of every release, with the HorizontalPodAutoscaler and the ServiceMonitor only where they were turned on. `kubeconform` downloads the schemas from two repositories on GitHub, each at a fixed commit, so a run without the network fails. Then it gives the chart what it has to refuse, and requires the reason in the words that the schema validator of Helm 4 has for it. A Helm before 3.18.5 has another validator, with other words for the same refusals.

- its own values alone, which are refused for each of the five values that an install names;
- an image tag that is a number;
- no replica at all;
- a password among the values, and one in the database URL;
- each of the five variables that the chart sets, as an entry of `extraEnv`;
- a Service of the type `LoadBalancer`;
- a grace period that leaves the drain 29 s;
- a cluster of Kubernetes 1.29.

`scripts/kind-smoke.sh` starts from nothing and uses what is in `deploy/kind`:

| File | What it is |
|---|---|
| `cluster.yaml` | The kind cluster: one node of Kubernetes 1.37, which publishes two node ports on ports of the loopback address that kind chooses. |
| `namespace.yaml` | The namespace of the test, which enforces the `restricted` Pod Security Standard. |
| `credentials.yaml` | The Secret, with the development values of `compose.yaml`. |
| `postgres.yaml` | The PostgreSQL of the test. Production uses a managed database. |
| `subscriber.yaml` | The subscriber of `examples/subscriber`, as `compose.yaml` runs it. |
| `values.yaml` | The values of the chart for the test: the image that the script builds, the database and the Secret above, a node port for the Service, and the `unsafe-local` profile with its allowance for `http` and for the addresses of the Services of the cluster, where the subscriber is. |

The script builds the image, creates the cluster and loads the image into its node. It installs PostgreSQL and the subscriber, waits for them, installs the chart with two replicas and waits for the rollout. Then it checks:

- **The migration was applied once.** Of the two replicas that started together, one logged the migration as applied and the other that none was necessary.
- **The replicas have a read-only root filesystem and no token.** The script reads from each that its root filesystem is read-only and that nothing is mounted in it but `/tmp`, which leaves no token for the API of Kubernetes.
- **The PodDisruptionBudget counts two healthy replicas,** so its selector finds them.
- **The API works through the Service.** `scripts/walkthrough.sh` is the run that `scripts/demo.sh` makes on the stack of `compose.yaml` (section 15): a tenant, an endpoint, a delivery that the subscriber verifies, a forged request that it refuses, and a delivery that fails once and succeeds with its retry. Every request to the API goes to the node port of the Service of the chart, which hands it to either replica.
- **Deliveries that are pending outlast a replica.** The subscriber is told to answer `503` and five events are sent. Their five failures open the circuit of the endpoint, so the five deliveries are pending and nothing is sent for them until a probe is due, 30 s later. The script deletes one of the two replicas, reads the five deliveries as still pending and tells the subscriber to answer `200` again. All five succeed.
- **A replica starts on a schema that is newer than its image.** Before it deletes the replica, the script adds the history row of a later migration. The replica that takes the place of the deleted one logs the warning of Flyway and becomes Ready.
- **No container of a replica was restarted.** A replica takes its restart count with it when it goes, so the script reads the counts twice. Before it deletes a replica, it reads those of the two that started together. At the end it reads those of the replica that stayed and of the one that took the place of the deleted one. A replica that lost the race for the migration and failed, or a probe that restarted a replica that works, would show in one of the two.

Deleting the replica does not depend on timing. The deliveries are pending for as long as the subscriber answers `503`, and it does until the replica is gone. It is also less than the failure table proves: no request is in flight when the replica is deleted. What a replica that dies in the middle of a request leaves is proven by rows 1 to 3 of the failure table with a SIGKILL, and what a replica that is stopped does by row 20. The smoke test shows that pending deliveries belong to no replica, on a cluster and through the chart.

The cluster stays when the script ends. `kind export kubeconfig --name reliable-webhooks-smoke` gives `kubectl` access to it, and `kind delete cluster --name reliable-webhooks-smoke` removes it. The next run removes it as well.

## A cluster and a database on AWS

`infra/terraform` is a Terraform module for what the chart needs on AWS: a network, an EKS cluster and a PostgreSQL instance on RDS. It has never been applied, and nothing in this repository has run on AWS. CI checks its format, validates it, plans it against a mocked provider and lints it ([What CI checks of it](#what-ci-checks-of-it)). None of that calls AWS. Nothing here therefore shows that AWS accepts what the module describes, and what this section says about the behaviour of AWS is taken from its documentation, from that of the AWS provider and from the modules that are called, not from a run.

### What the module creates

It calls three community modules, each at a fixed version, with the AWS provider 6.68.0: `terraform-aws-modules/vpc` 6.7.3, `terraform-aws-modules/eks` 21.29.0 and `terraform-aws-modules/rds` 7.2.2. The security group and the parameter group of the database, and the resource that turns the rotation of its password off, are its own. The lock file fixes the further providers that those modules bring.

- **A VPC** over three availability zones, with a private and a public subnet in each. The nodes, the pods and the database are in the private subnets and have no public address. The public subnets hold the NAT gateways, which are the way out for what the nodes pull and for the deliveries that the service sends to subscribers. Each zone has its own gateway, or all three share one (`single_nat_gateway`).
  - The range of the VPC (`vpc_cidr`) has to lie in 10.0.0.0/8, 172.16.0.0/12 or 192.168.0.0/16. A replica connects to no address of these ranges (section 11.4), so a subscriber URL that resolves to a node, a pod or the database is refused like any other private address.
- **An EKS cluster** of Kubernetes 1.37, which is the version that the smoke test installs the chart on, with one managed node group in the private subnets, on the Amazon Linux 2023 image for x86_64, and with the three add-ons that a node needs: the VPC CNI, kube-proxy and CoreDNS.
  - The API server is reached from outside the VPC only from the address ranges that `cluster_endpoint_public_access_cidrs` names. It names none unless it is set, and the API server then has no public endpoint.
  - Inside the VPC the API server admits the nodes of the cluster and no other machine. The security group that the cluster module gives it has one rule, for port 443 from the security group of the nodes, and `main.tf` adds none. `kubectl` and `helm` on another machine in the VPC therefore need a rule on that security group, which is made by hand.
  - Whoever applies the module is the administrator of the cluster. No other identity is given access to it.
  - The cluster module encrypts the Secrets of the cluster with a KMS key that it creates, which covers the Secret of the chart.
- **A PostgreSQL 18.6 instance on RDS** in the private subnets, with encrypted storage, automated backups that are kept for 7 days unless `database_backup_retention_days` says otherwise, deletion protection, and a standby in a second zone unless `database_multi_az` is false. 18.6 is the version that the tests run against. RDS is told not to move the instance to another minor version by itself, so another version is a change to `main.tf`.
  - **The user.** The master user of the instance is `webhooks`, and it is the user that the service connects as, because that user has to own the schema ([What the chart needs](#what-the-chart-needs)). The module creates no other user. RDS creates the password and keeps it in Secrets Manager. It is in no variable, in no output and not in the state. RDS would also rotate it every seven days, and the module turns that rotation off ([The password of the database](#the-password-of-the-database)).
  - **The parameter group** sets `default_transaction_isolation` to `read committed` and `synchronous_commit` to `on`. These are the two settings of the database that a replica checks when it starts (section 15).
  - **The security group** admits TCP port 5432 from the security group of the nodes and from nothing else. A pod has the security group of its node, so this is what lets the replicas in.
  - **TLS.** The URL that the module hands to the chart says `sslmode=require`, so a replica connects over TLS or not at all, and it does not check the certificate of the instance ([What the chart needs](#what-the-chart-needs)). By the documentation of RDS, PostgreSQL 15 and later refuses a connection without TLS unless `rds.force_ssl` is turned off, and the parameter group leaves that setting alone.
- **Tags.** The provider puts `Deployment`, with the name of the deployment, `ManagedBy` and the tags of `tags` on every resource that it creates and that can carry tags. The instances, volumes and network interfaces of the nodes get them from the launch template of the node group.

It creates no registry for the image, no ingress and no load balancer, nothing that adds nodes, and neither the Secret nor the release of the chart. The role of the nodes may pull from a registry of ECR in the same account. Publishing the API stays what [What the chart leaves to the cluster](#what-the-chart-leaves-to-the-cluster) says it is.

### The values and the state

`variables.tf` has the variables, each with what it is for. All but the two switches, `single_nat_gateway` and `database_multi_az`, have a rule that refuses a value outside it. The region and the sizes have no default, so nothing is planned until they are chosen: the instance types and the two limits of the node group, and the class and the storage of the database. `example.tfvars` is a small deployment, with two nodes, one NAT gateway and a database without a standby.

```
cd infra/terraform
cp example.tfvars terraform.tfvars
terraform init
terraform plan
```

git ignores `terraform.tfvars`, the `.terraform` directory and state files. No credential belongs in any of them: Terraform takes the credentials for AWS from its environment.

The module names no backend. Left like that, Terraform keeps the state in a file next to the module, on the machine that applied it. A deployment that lasts keeps its state where it is shared and locked, which for a bucket of S3 is a file `backend.tf` next to `main.tf`:

```hcl
terraform {
  backend "s3" {
    bucket       = "the-bucket-of-the-state"
    key          = "reliable-webhooks/terraform.tfstate"
    region       = "eu-west-1"
    use_lockfile = true
  }
}
```

The state holds no password. It does name the account and every resource of the deployment.

### From the outputs to the chart

| Output | Where it goes |
|---|---|
| `cluster_name` | `aws eks update-kubeconfig --name`, which gives `kubectl` and `helm` the cluster |
| `database_url` | The value `database.url` of the chart |
| `database_username` | The value `database.username` of the chart |
| `database_password_secret_arn` | The secret in Secrets Manager whose member `password` becomes the key `database-password` of the Secret |

With them, the commands of [What the chart needs](#what-the-chart-needs) and of [Installing](#installing) become these, run in `infra/terraform`, with the region of `terraform.tfvars` in place of `eu-west-1`:

```
aws eks update-kubeconfig --region eu-west-1 --name "$(terraform output -raw cluster_name)"

kubectl create namespace webhooks
kubectl --namespace webhooks create secret generic reliable-webhooks-credentials \
  --from-literal=database-password="$(aws secretsmanager get-secret-value --region eu-west-1 \
    --secret-id "$(terraform output -raw database_password_secret_arn)" \
    --query SecretString --output text | jq --raw-output .password)" \
  --from-literal=admin-token="$(openssl rand -hex 32)" \
  --from-literal=secret-encryption-key="$(openssl rand -base64 32)"

helm install reliable-webhooks ../../deploy/helm/reliable-webhooks \
  --namespace webhooks \
  --set image.repository=registry.example.com/reliable-webhooks \
  --set-string image.tag=1 \
  --set database.url="$(terraform output -raw database_url)" \
  --set database.username="$(terraform output -raw database_username)" \
  --set secret.name=reliable-webhooks-credentials
```

These commands have not been run, because the module has not been applied. One thing about them is checked. The test of the CI job plans `database_url` as `jdbc:postgresql://`, the endpoint of the instance with its port, `/webhooks` and `?sslmode=require`, without a user and without a password, which is the form that the schema of the chart takes.

The Secret holds the password as it is when the command runs. Nothing changes that password afterwards unless somebody rotates it by hand ([The password of the database](#the-password-of-the-database)).

### The password of the database

RDS rotates a master password that it manages, every seven days unless the schedule of its secret is changed. The Secret of the chart holds a copy of the password, a replica reads it once, when it starts, and nothing in this repository carries a new password into the Secret. `main.tf` therefore turns the rotation off: its resource `aws_secretsmanager_secret_rotation.database` sets `rotation_enabled = false` on the secret that `database_password_secret_arn` names. With the rotation off, the password that the Secret holds stays the password of the user.

- **Where the switch comes from.** It is an argument of the AWS provider, whose documentation gives it for the secret of a master password that RDS manages. The RDS module turns a rotation off only by removing a schedule that an earlier apply has set, which is why the resource is in `main.tf`. The user guide of RDS speaks only of changing the schedule. So read the setting from the secret once the module has been applied:

  ```
  aws secretsmanager describe-secret --region eu-west-1 \
    --secret-id "$(terraform output -raw database_password_secret_arn)" \
    --query RotationEnabled
  ```

  The answer has to be `false`. The provider reads the same setting whenever Terraform plans, so a rotation that is on again shows in `terraform plan` as a change, and `terraform apply` turns it off.
- **What it costs.** Nothing rotates the password any more. It is as old as the instance until somebody rotates it by hand, and a copy of it that got out is valid for that long. It is the password of the user that owns the schema, and the instance has no other user.

Rotating the password by hand takes four steps, one right after the other, run in `infra/terraform`:

1. RDS gives the user a new password and puts it into the secret as a new version. Read the version that the secret has now:

   ```
   aws secretsmanager get-secret-value --region eu-west-1 \
     --secret-id "$(terraform output -raw database_password_secret_arn)" \
     --query VersionId --output text
   ```

   Then ask RDS for the rotation. The identifier of the instance is the `name` of the deployment:

   ```
   aws rds modify-db-instance --region eu-west-1 \
     --db-instance-identifier reliable-webhooks \
     --rotate-master-user-password --apply-immediately
   ```

   By the documentation of RDS the instance reports the secret as `rotating` while that goes on, and as `active` when it is over. `active` is also what it reports before a rotation, so that answer alone does not show that this one is over. Wait until the first command names another version and this one answers `active`:

   ```
   aws rds describe-db-instances --region eu-west-1 \
     --db-instance-identifier reliable-webhooks \
     --query 'DBInstances[0].MasterUserSecret.SecretStatus' --output text
   ```

2. The new password replaces the key `database-password` of the Secret. The other two keys stay as they are, which the encryption key has to ([What the chart needs](#what-the-chart-needs)):

   ```
   kubectl --namespace webhooks patch secret reliable-webhooks-credentials --type merge \
     --patch "$(aws secretsmanager get-secret-value --region eu-west-1 \
       --secret-id "$(terraform output -raw database_password_secret_arn)" \
       --query SecretString --output text \
       | jq '{stringData: {"database-password": .password}}')"
   ```

3. The replicas are restarted, so that they read the Secret. The Deployment of the release that was installed above is `reliable-webhooks`:

   ```
   kubectl --namespace webhooks rollout restart deployment/reliable-webhooks
   kubectl --namespace webhooks rollout status deployment/reliable-webhooks
   ```

4. The rotation is turned off again. By the documentation of Secrets Manager, the call that starts a rotation is also the one that turns the automatic rotation on, and the user guide of RDS requires the permission for that call of whoever rotates the password. So expect the rotation to be on again after step 1. Terraform finds that as a change of `aws_secretsmanager_secret_rotation.database`, and the target keeps the apply to that resource and to what it depends on. The cluster is not part of that, so the newer version of an add-on or of the node image that a plan of the whole module can hold by then is not applied with it ([What to check before it is applied](#what-to-check-before-it-is-applied)):

   ```
   terraform apply -target=aws_secretsmanager_secret_rotation.database
   ```

   The `describe-secret` command above then has to answer `false` again.

The steps belong together because of what a rotation does to the replicas that are running:

- A connection that is open outlasts a rotation, and a new one is refused with the old password. The pools of a replica replace a connection when it is 30 minutes old, which is the default of HikariCP that the service leaves in place.
- From step 1 on the pools therefore lose their connections one by one, and about half an hour later they are empty. No call of a replica then gets a connection inside its borrow wait. A route answers that with `503 database_busy` (section 5), and nothing is claimed or delivered.
- Neither probe fails, because neither asks the database ([Probes](#probes)), so nothing restarts a replica, and a restart alone would not help. The replicas work again once the Secret has the new password and they have been restarted.
- The restart of step 3 is a rollout: a replica with the new password is Ready before a replica with the old one is stopped ([Values](#values)).
- A rotation that step 4 does not turn off rotates the password again on the schedule of the secret, and nobody follows that rotation with steps 2 and 3.

None of these steps has been run on AWS, because the module has not been applied.

### What to check before it is applied

- **The quotas of the account in the region.** The nodes count against the vCPUs of running On-Demand instances, each NAT gateway takes an Elastic IP address, and a region has a limit on its VPCs. An apply that meets a quota stops halfway.
- **The versions.** Kubernetes 1.37 and PostgreSQL 18.6 are written in `main.tf`, and AWS has to offer both in the region (`aws eks describe-cluster-versions`, `aws rds describe-db-engine-versions --engine postgres`). Two versions are not written there, because they depend on the Kubernetes version: each add-on has the version that EKS names as its default for it, and the nodes have the newest release of their image for it. Both are looked up when the module is planned, so a later plan can bring a newer one.
- **The way to the API server.** `kubectl` and `helm` have to reach it before anything is installed. From outside the VPC that takes the range that they are run from in `cluster_endpoint_public_access_cidrs`. From a machine inside the VPC it takes a rule on the security group of the cluster that the module does not create ([What the module creates](#what-the-module-creates)). With the variable left as it is and no such rule, nothing but the nodes reaches the API server.
- **The nodes against the replicas.** A replica requests half a CPU and 1 GiB ([Values](#values)), a rollout runs one replica more than `replicaCount`, and the add-ons of the cluster run on the same nodes. The node group starts with `node_min_size` nodes, and nothing adds one: with the autoscaler of the chart on, a replica that finds no room stays Pending. The number of nodes is changed with `aws eks update-nodegroup-config`, between `node_min_size` and `node_max_size`. Terraform sets the number once, when the node group is created, and leaves it alone after that. Raise the number before `node_min_size` is raised above it: Terraform does not send the number with the new limit, and EKS refuses a smallest number of nodes above the number that the group has.
- **`max_connections` against the replicas.** A replica holds 32 connections, and the database needs room for 32 times twice the largest number of replicas, and a few on top ([What the chart needs](#what-the-chart-needs)). For the two replicas of the chart that is 128 and a few. The parameter group does not set `max_connections`, so RDS derives it from the memory of `database_instance_class`. Read it from the instance before the chart is installed (`SHOW max_connections`), and take a larger class if it is too small.
- **The storage against the tenants.** The bodies that one tenant stores are bounded by its rate, at the default rate by about 16 GiB a day, and they are kept for 30 days (section 12). `database_allocated_storage` is all that the instance has: the module does not let RDS add to it.
- **The architecture of the image.** The nodes are x86_64. An image that is built on another architecture needs `docker build --platform linux/amd64`. The stage of the `Dockerfile` that runs Maven stays on the architecture of the machine that builds, and only the stage that the image is made of is taken for the platform that was asked for. CI builds the image on x86_64, so no job builds it for another architecture than that of its runner.

### What CI checks of it

The job `terraform` of the CI workflow runs these in `infra/terraform`, without credentials:

```
terraform fmt -check -recursive -diff
terraform init -backend=false -lockfile=readonly -input=false
terraform validate
terraform test -var-file=example.tfvars
tflint --init
tflint
tflint --only aws_db_instance_invalid_type --var database_instance_class=db.t4g.none
```

- **`terraform init`** installs the three modules and the providers that the lock file names. `-lockfile=readonly` keeps it from rewriting the lock file, so a provider that the file does not name fails the job.
- **`terraform validate`** holds the module against the schemas of the provider and of the three modules at their fixed versions.
- **`terraform test`** runs `tests/plan.tftest.hcl`, in which every run is a plan against a mocked provider.
  - With the values of `example.tfvars` the module is planned. The subnets of the VPC are the six pieces of the range that they have to be, and it has one NAT gateway. The rule of the database is TCP port 5432 from the security group that the cluster module creates for the nodes, and from no address range and no prefix list. The parameter group sets the two settings and no other, and `database_url` has the form above. `database_password_secret_arn` names the secret that the instance reports for its master user. The resource of `main.tf` that turns the rotation off names the same secret, sets `rotation_enabled = false` and has no schedule, and the RDS module schedules no rotation of its own.
  - A second run plans the three defaults that the example replaces: an API server without a public endpoint, a NAT gateway in each zone, which the run counts, and a database with a standby.
  - Further runs give the variables a value at each of their limits, which is accepted, and every rule a value beyond its limit or of the wrong form, which is refused. One run answers the module with a region of two availability zones, which is refused as well.
  - No run asserts what `main.tf` hands to the RDS module and to the cluster module: the encryption, the deletion protection, the backups, the managed password and the private placement of the instance, and the access to the API server. A run reads the resources and the outputs of the module itself, and no resource inside a module that it calls. Those settings are what `main.tf` says and are held to nothing else. The secret of the master user is an answer of the mocked provider, which gives one whatever the instance is planned with.
  - No run reads the tags of a planned resource. A mocked provider does not work out the tags that a resource ends up with, and the run about the tags holds the module to the order in which it merges them.
- **`tflint`** runs its Terraform rules and the rules of its AWS plugin, version 0.49.0. It is told to follow the values of `example.tfvars` into the three modules, which is where the engine and the instance class of the database are used. The last command shows that it follows a value that far: the database instance is declared two module calls below `main.tf`, and with an instance class that RDS does not have, the rule for instance classes has to refuse it. The step requires the exit code 2, with which tflint reports an issue.

A mocked provider answers whatever it is asked. The plans show that the module can be evaluated with the example values and with its defaults, and that its rules hold. They do not show that an apply succeeds.
