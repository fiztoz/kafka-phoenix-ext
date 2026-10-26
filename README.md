# kafka-phoenix-ext

Custom Uptime Phoenix extension: Kafka per-topic **storage usage** dashboard /
wallboard. Phoenix core stays health-only; Kafka metering lives here, in its
own image. Mirrors the [`ecs-phoenix-ext`](https://github.com/fiztoz/ecs-phoenix-ext)
architecture (poller + MariaDB/SQLite store + iframe dashboard + monitor health
endpoints).

- Go 1.27, `CGO_ENABLED=0`, stdlib `net/http`, server-rendered templates.
- **Polls several Kafka clusters from one process** (`KAFKA_CLUSTERS`), each
  with its own brokers, credentials, topic filter, thresholds, hysteresis and
  growth window. See [Multiple clusters](#multiple-clusters).
- Polls the Kafka admin protocol **only** (read-only):
  - `DescribeLogDirs` (per-broker log sizes) via `franz-go` `kadm` — no JMX
    exporter, Jolokia or SSH access to brokers required. Kafka ≥ 1.0.
  - `Metadata` for partition leaders / replicas / ISR and topic internals.
- Required ACLs for the service principal: **Describe** on `CLUSTER` (brokers)
  and **Describe** on `TOPIC` (topics). Nothing else is used.

## What it shows

- Per topic: size in bytes of **all replica copies** (`storage_bytes`, the
  cluster-wide disk footprint), leader-only logical size (`leader_bytes`),
  partitions, average replication factor, growth per hour, under-replicated
  / offline partitions.
- Per broker: total bytes and hosted replica-copy count (disk balancing).
- Per partition drill-down (`/topic/{name}`): leader, replicas, ISR, size,
  offset lag, under-replicated/offline flags.
- Per broker drill-down (`/broker/{id}`): share of cluster bytes, hosted
  topics and largest partitions led on that broker.
- Operator-set **size thresholds** with 2-sample hysteresis → `/health/thresholds`
  goes 503 so a Phoenix HTTP monitor can alert; warn level below the limit.
- Optional **growth thresholds** (bytes/hour over a rolling window) with
  time-to-limit forecasts → `/health/growth`.
- **Replica health** monitor: `/health/replicas` 503 when under-replicated or
  offline partitions persist across polls.
- **Broker skew** monitor: `/health/brokers` 503 when one broker holds more
  than `KAFKA_BROKER_SKEW_PCT` of cluster bytes (or `KAFKA_BROKER_MAX_BYTES`).
- Partition skew (max/avg leader partition size) per topic, sortable on the
  dashboard; failed polls are classified (auth vs timeout vs network) with
  the required ACLs shown next to auth errors.

## Local run (SQLite, no MariaDB needed)

```bash
export KAFKA_BROKERS=kafka-0.example.com:9092,kafka-1.example.com:9092
export KAFKA_SASL_MECHANISM=none            # none | plain | scram-sha-256 | scram-sha-512
export DATABASE_DSN="file:./kafka-phoenix-ext.db"
export BASE_PATH=/                          # serve at / for go run
make run
# dashboard: http://localhost:8080/   (LISTEN_ADDR=:8080 for local)
# wallboard: http://localhost:8080/wallboard
```

Health probes (always open, no UI token):

```bash
curl -s localhost:8080/health/live
curl -s localhost:8080/health/ready        # 503 while Kafka unreachable
curl -s localhost:8080/health/thresholds   # 503 when any topic confirmed over
curl -s localhost:8080/health/replicas     # 503 on confirmed under-rep/offline
curl -s localhost:8080/health/growth       # 503 on confirmed growth over limit
curl -s localhost:8080/health/brokers      # 503 on confirmed broker skew
```

With more than one cluster the same five monitors are also served per cluster,
which is what a Phoenix monitor should usually point at:

```bash
curl -s localhost:8080/health/clusters            # 503 while ANY cluster is unpollable
curl -s localhost:8080/health/c/harbor/ready       # 503 while harbor is unpollable
curl -s localhost:8080/health/c/harbor/thresholds  # 503 when a harbor topic is over
curl -s localhost:8080/health/c/harbor/replicas
curl -s localhost:8080/health/c/harbor/growth
curl -s localhost:8080/health/c/harbor/brokers
```

The unprefixed paths above stay bound to one *legacy* cluster so existing
monitors keep working after a multi-cluster migration (see
`KAFKA_LEGACY_CLUSTER`).

## Tests / lint

```bash
make test    # fixture aggregation tests; no live Kafka needed
make vet
```

## Environment

Cluster ids are declared in `KAFKA_CLUSTERS`; every cluster-scoped variable is
then addressed as `KAFKA_<ID>_<NAME>`. Any per-cluster variable left unset
falls back to the shared `KAFKA_<NAME>` value below, so clusters sharing one
service principal only need their own broker list.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `KAFKA_CLUSTERS` | no | — | comma-separated cluster ids, e.g. `harbor,summit`; unset keeps the single-cluster layout |
| `KAFKA_LEGACY_CLUSTER` | no | `default` if present, else first cluster | which cluster serves the unprefixed `/health/ready`, `/topic/*`, `/api/*` routes |
| `KAFKA_BROKERS` | yes* | — | comma-separated seed brokers, no scheme (*per-cluster `KAFKA_<ID>_BROKERS` when `KAFKA_CLUSTERS` is set) |
| `KAFKA_SASL_MECHANISM` | no | `none` | `none`, `plain`, `scram-sha-256`, `scram-sha-512` |
| `KAFKA_SASL_USERNAME` | when SASL | — | service principal |
| `KAFKA_SASL_PASSWORD` | when SASL | — | from Secret (`sasl-password`); never logged |
| `KAFKA_TLS_ENABLED` | no | `false` | dial brokers with TLS; strongly recommended with SASL |
| `KAFKA_TLS_CA_FILE` | no | — | CA PEM file (takes precedence over inline) |
| `KAFKA_TLS_CA` | no | — | inline CA PEM; no volume support |
| `KAFKA_INCLUDE_INTERNAL` | no | `false` | show `__consumer_offsets` etc. |
| `KAFKA_TOPIC_FILTER` | no | `.*` | regexp; non-matching topics are hidden |
| `KAFKA_POLL_INTERVAL` | no | `15m` | poll cadence; min `1m` |
| `KAFKA_TIMEOUT` | no | `45s` | per-describe deadline; max `10m` |
| `KAFKA_BROKER_SKEW_PCT` | no | `60` | one broker over this % of cluster bytes is skew (needs ≥2 brokers) |
| `KAFKA_BROKER_MAX_BYTES` | no | `0` | absolute per-broker byte limit; `0` disables |
| `DATABASE_DSN` | yes | — | MariaDB DSN or `file:` SQLite path |
| `DATABASE_ENGINE` | no | inferred | `mariadb` \| `sqlite` |
| `BASE_PATH` | no | `/kafka` | must equal the chart `path` value |
| `LISTEN_ADDR` | no | `:8080` | non-root ports only in the container |
| `UI_TOKEN` | no | — | optional bearer auth for dashboard/APIs (health stays open) |
| `LOG_LEVEL` | no | `info` | slog level |

`KAFKA_SASL_*`, `KAFKA_TLS_*`, `KAFKA_TOPIC_FILTER` and
`KAFKA_INCLUDE_INTERNAL` are shared defaults; prefix any of them with a
cluster id to override it for that cluster (`KAFKA_SUMMIT_SASL_USERNAME`,
`KAFKA_SUMMIT_TLS_ENABLED=false`, ...). `KAFKA_POLL_INTERVAL`, `KAFKA_TIMEOUT`
and the broker-skew policy are global.

Secrets arrive via K8s `secretKeyRef` / chart `envFromSecret`; there is no
inline credential support. See the chart wiring section below.

## Multiple clusters

One extension process, one database, and one HTTP endpoint can monitor several
Kafka clusters. `harbor` and `summit` throughout this guide are **fictional
names**; replace their broker addresses, Secret names, and cluster ids with
your own. Cluster ids are durable storage keys, not Kafka's reported cluster
UUIDs, so choose stable names.

**Existing installation?** Follow [Upgrading an existing single-cluster
installation](#upgrading-an-existing-single-cluster-installation) first. The
fresh-install examples below must not be used to rename an existing cluster,
otherwise its stored thresholds would no longer be selected.

### 1. Prepare connectivity and access

For each Kafka cluster:

- Collect its bootstrap brokers, listener port, SASL mechanism, service
  principal, and TLS CA certificate (if not trusted by the system CA pool).
- Grant the monitoring principal **Describe on CLUSTER** and **Describe on
  TOPIC** for the topics you want to monitor. No write ACLs, JMX, or broker
  SSH access are needed.
- Allow the extension to resolve and reach **every advertised broker
  address**, not just the bootstrap address. Remote Kubernetes Service DNS
  names are not automatically resolvable from another Kubernetes cluster;
  use a listener whose advertised hostnames and ports are reachable from
  where the extension runs.
- Allow DNS, Kafka listener ports, and database access in NetworkPolicies
  and firewalls. See [chart wiring](docs/chart-wiring.md) for the additional
  Kafka egress policy needed by the Phoenix chart.

Use a binary/image built from a revision containing multi-cluster support.
Older release images do not gain this feature just by setting these variables.
Use one extension replica with its dedicated database; see [Database](#database)
for MariaDB provisioning. SQLite is suitable for the local example below.

### 2. Try a fresh local installation

This example is only for test Kafka listeners deliberately configured
**without SASL or TLS**. For authenticated listeners, use the settings in
step 3 instead; do not disable broker security just to run this example.
Start with a new SQLite database path.

```bash
unset KAFKA_BROKERS
export KAFKA_CLUSTERS=harbor,summit
export KAFKA_HARBOR_BROKERS=harbor-kafka-0.example.com:9092,harbor-kafka-1.example.com:9092
export KAFKA_SUMMIT_BROKERS=summit-kafka-0.example.com:9092
export KAFKA_SASL_MECHANISM=none
export KAFKA_TLS_ENABLED=false
export DATABASE_DSN="file:./kafka-multicluster.db"
export DATABASE_ENGINE=sqlite
export KAFKA_POLL_INTERVAL=1m
export BASE_PATH=/kafka
export LISTEN_ADDR=127.0.0.1:8080
make run
```

Open `http://localhost:8080/kafka/` for the overview, then select a cluster.
The two dashboards are `/kafka/c/harbor` and `/kafka/c/summit`. On a fresh
process, readiness remains 503 until each cluster has a successful poll.

### 3. Configure SASL/TLS for Kubernetes

Set the variables below in the extension container's `env` list (or the
Phoenix chart extension's `env` list). This example uses SCRAM-SHA-512 and
TLS for both clusters, with separate credentials and CA certificates. Broker
names and Secret names are illustrative.

Before starting the pod, provision these Secrets in its namespace using your
normal secret-management workflow; do **not** commit credential values:

| Secret | Required keys | Purpose |
|---|---|---|
| `kafka-harbor-client` | `username`, `password`, `ca.pem` | harbor SASL principal and PEM CA bundle |
| `kafka-summit-client` | `username`, `password`, `ca.pem` | summit SASL principal and PEM CA bundle |
| `kafka-phoenix-ext-db` | `dsn` | dedicated MariaDB schema DSN |

```yaml
env:
  - name: KAFKA_CLUSTERS
    value: "harbor,summit"
  - name: KAFKA_HARBOR_BROKERS
    value: "harbor-kafka-0.example.com:9094,harbor-kafka-1.example.com:9094"
  - name: KAFKA_SUMMIT_BROKERS
    value: "summit-kafka-0.example.com:9094"
  # Shared defaults inherited by both clusters.
  - name: KAFKA_SASL_MECHANISM
    value: "scram-sha-512"
  - name: KAFKA_TLS_ENABLED
    value: "true"
  - name: KAFKA_HARBOR_SASL_USERNAME
    valueFrom:
      secretKeyRef:
        name: kafka-harbor-client
        key: username
  - name: KAFKA_HARBOR_SASL_PASSWORD
    valueFrom:
      secretKeyRef:
        name: kafka-harbor-client
        key: password
  - name: KAFKA_HARBOR_TLS_CA
    valueFrom:
      secretKeyRef:
        name: kafka-harbor-client
        key: ca.pem
  - name: KAFKA_SUMMIT_SASL_USERNAME
    valueFrom:
      secretKeyRef:
        name: kafka-summit-client
        key: username
  - name: KAFKA_SUMMIT_SASL_PASSWORD
    valueFrom:
      secretKeyRef:
        name: kafka-summit-client
        key: password
  - name: KAFKA_SUMMIT_TLS_CA
    valueFrom:
      secretKeyRef:
        name: kafka-summit-client
        key: ca.pem
  - name: DATABASE_DSN
    valueFrom:
      secretKeyRef:
        name: kafka-phoenix-ext-db
        key: dsn
  - name: DATABASE_ENGINE
    value: "mariadb"
  - name: BASE_PATH
    value: "/kafka"
  - name: LISTEN_ADDR
    value: ":8080"
```

Remove any old `KAFKA_BROKERS` entry, including one injected through `envFrom`;
it cannot coexist with `KAFKA_CLUSTERS`. Keep pod liveness/readiness probes on
`/kafka/health/live` (Phoenix chart: `readinessPath: /health/live`) so one
Kafka outage does not remove the cached dashboard from the Service.
For production access, set `UI_TOKEN` through a Secret reference and use the
same token in the Phoenix extension integration. Without it, dashboards,
APIs, and threshold mutations are open. Health endpoints remain unauthenticated
by design; restrict their exposure with your Ingress/firewall policy.

If both clusters really share a principal, use shared
`KAFKA_SASL_USERNAME`/`KAFKA_SASL_PASSWORD` Secret references instead of
repeating per-cluster credentials. Otherwise keep the separate references
above. A cluster can override the mechanism too, for example
`KAFKA_SUMMIT_SASL_MECHANISM=plain` for a TLS listener that uses SASL/PLAIN.

Per-cluster values override shared defaults. For example,
`KAFKA_SUMMIT_TOPIC_FILTER=^orders[.]` restricts summit's visible topics,
while `KAFKA_HARBOR_INCLUDE_INTERNAL=true` includes harbor's internal topics.
Poll cadence, Kafka timeout, and broker-skew policy remain global.
String overrides must be non-empty; an empty value falls back to the shared
setting. Boolean overrides can explicitly set `false`. A configured
`TLS_CA_FILE` takes precedence over inline `TLS_CA`; when using the inline
CA example above, remove any inherited CA-file setting, or provide the
correct CA file separately for each cluster. Omit custom CA settings when
the brokers are already trusted by the system CA pool.

### 4. Verify each cluster and create monitors

With the local example running, or after forwarding the extension Service
to localhost, check:

```bash
curl -i http://localhost:8080/kafka/health/live
curl -i http://localhost:8080/kafka/health/clusters
curl -i http://localhost:8080/kafka/health/c/harbor/ready
curl -i http://localhost:8080/kafka/health/c/summit/ready
```

`live` checks the process only. `ready` checks the selected cluster's polling
status; `clusters` is 503 if any configured cluster's latest poll failed or
has not succeeded yet. These health endpoints stay open even with `UI_TOKEN`
configured; dashboard and API requests require authentication when it is set.

Create separate Phoenix HTTP monitors for each cluster rather than relying
on the legacy unprefixed routes:

| Monitor | harbor path | summit path |
|---|---|---|
| Kafka polling | `/kafka/health/c/harbor/ready` | `/kafka/health/c/summit/ready` |
| Topic size | `/kafka/health/c/harbor/thresholds` | `/kafka/health/c/summit/thresholds` |
| Replica health | `/kafka/health/c/harbor/replicas` | `/kafka/health/c/summit/replicas` |
| Topic growth | `/kafka/health/c/harbor/growth` | `/kafka/health/c/summit/growth` |
| Broker skew | `/kafka/health/c/harbor/brokers` | `/kafka/health/c/summit/brokers` |

On each cluster's topic page, set size/warn/growth thresholds as needed. The
same topic name in two clusters has independent limits and history. Capacity
and growth alerts use consecutive-poll confirmation; setting a limit alone
does not manufacture a new observation. Alert endpoints retain their last
observed state during failed polls, so monitor each cluster's `ready` path
as well. Add `/kafka/health/clusters` if you also want one aggregate polling
monitor. Replace `/kafka` with your configured `BASE_PATH` throughout.

### Configuration rules and limits

- Cluster ids match `[a-z][a-z0-9_-]*` (max 32 chars) and appear in URLs;
  `prod-eu` is addressed as `KAFKA_PROD_EU_*`. IDs that map to the same
  environment prefix (such as `prod-eu` and `prod_eu`) cannot coexist.
- Each cluster needs its own `KAFKA_<ID>_BROKERS`; a shared `KAFKA_BROKERS` is
  **not** reused for brokers and is rejected alongside `KAFKA_CLUSTERS`.
- At most 20 clusters, one poller goroutine each.
- A failed poll in one cluster does not stop the other pollers. Its snapshot
  keeps the last good sample; its readiness endpoint and `/health/clusters`
  report the polling failure. Alert endpoints retain their last observed state.

### URLs

| Route | Page |
|---|---|
| `/kafka/` | cluster overview (single cluster: its dashboard) |
| `/kafka/c/{cluster}` | that cluster's dashboard |
| `/kafka/c/{cluster}/wallboard` | wallboard for one cluster |
| `/kafka/c/{cluster}/topic/{name}` | topic drill-down |
| `/kafka/c/{cluster}/broker/{id}` | broker drill-down |
| `/kafka/c/{cluster}/api/topics`, `/kafka/c/{cluster}/api/brokers` | JSON for that cluster (carries `cluster`) |

A **single-cluster** install keeps the original flat URLs (`/kafka/topic/x`,
`/kafka/wallboard`); nothing has to change on upgrade. The `/c/{cluster}/`
routes still resolve there, and they become the canonical links as soon as a
second cluster is configured.

### Storage

Both tables are keyed by `(cluster, topic)`, so two clusters with a topic of
the same name never share a size row or a threshold. Migration 004 (MariaDB)
and 005 (SQLite table rebuild) add the column and swap the primary key;
existing rows are labelled `default`, which is the id the single-cluster
layout uses, so thresholds and hysteresis survive the upgrade **when the
original cluster keeps the id `default`**. SQLite applies its rebuild and the
version record in one transaction. MariaDB's cluster migration can resume
from a partially applied DDL attempt or a missing version record.

### Upgrading an existing single-cluster installation

1. Stop the extension and back up its dedicated database before upgrading.
2. Keep the original Kafka cluster's id as `default`. Replace `KAFKA_BROKERS`
   with `KAFKA_DEFAULT_BROKERS`, keeping the **exact same broker list**. Do not
   leave `KAFKA_BROKERS` set alongside `KAFKA_CLUSTERS`.
3. Add the new cluster with a fictional example such as:

   ```bash
   export KAFKA_CLUSTERS=default,summit
   export KAFKA_DEFAULT_BROKERS="$KAFKA_BROKERS"
   unset KAFKA_BROKERS
   export KAFKA_SUMMIT_BROKERS=summit-kafka.example.com:9092
   export KAFKA_LEGACY_CLUSTER=default
   ```

   Keep the original cluster's SASL/TLS/filter settings; give the new cluster
   its own overrides where they differ. Never point `default` at the new
   cluster: this id owns the existing thresholds and observation history.
4. Start the upgraded extension. With the default `BASE_PATH=/kafka`, check
   `/kafka/health/c/default/ready`, the old unprefixed-cluster monitors such
   as `/kafka/health/ready`, and the original topic thresholds before adding
   new monitors under `/kafka/health/c/summit/*`. Substitute your own
   `BASE_PATH` if different.

Changing `KAFKA_LEGACY_CLUSTER` selects HTTP routes only; it **does not move
stored rows**. Startup refuses a configuration that omits `default` while
that scope still has state or thresholds, rather than silently disabling
those settings. Fresh databases may use arbitrary valid cluster ids. Renaming
a populated cluster is not an automatic operation; retain its existing id.

## Database

Use a dedicated MariaDB schema `kafka_usage`, not Phoenix's application
schema. Apply `deploy/grants.sql` after substituting the user's password
from your vault, then point `DATABASE_DSN` at that schema. The extension
creates its own tables on startup (embedded migrations run before the first
poll), including `ext_kafka_usage_schema_migrations`. The schema-level grant
covers tables added by later migrations, so a new migration does not need a
GRANT edit. Do not grant `phoenix.*` and do not share Phoenix credentials.

Topic primary keys are case-sensitive (`utf8mb4_bin` on the dedicated
schema; SQLite's default BINARY collation). Migration 003 converts existing
MariaDB topic columns. If `Orders` and `orders` already collided under a
case-insensitive collation, only the stored spelling survives — the lost
row cannot be reconstructed.

If a dedicated schema is impossible, grant each current extension table
listed in `deploy/grants.sql` explicitly and repeat that GRANT before
deploying a migration that creates another table. A table-prefix pattern
such as `phoenix.ext_kafka_usage_%` is not valid MariaDB syntax.

The store auto-appends `parseTime=true&multiStatements=true` when missing.
Chart integration (values.yaml example, port override, NetworkPolicy gotcha)
is documented in [`docs/chart-wiring.md`](docs/chart-wiring.md).
Suggested next features (consumer lag, replica/broker health monitors, and
what is explicitly out of scope) live in
[`docs/feature-suggestions.md`](docs/feature-suggestions.md).