# kafka-phoenix-ext

Custom Uptime Phoenix extension: Kafka per-topic **storage usage** dashboard /
wallboard. Phoenix core stays health-only; Kafka metering lives here, in its
own image. Mirrors the [`ecs-phoenix-ext`](https://github.com/fiztoz/ecs-phoenix-ext)
architecture (poller + MariaDB/SQLite store + iframe dashboard + monitor health
endpoints).

- Go 1.27, `CGO_ENABLED=0`, stdlib `net/http`, server-rendered templates.
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
- Operator-set **size thresholds** with 2-sample hysteresis → `/health/thresholds`
  goes 503 so a Phoenix HTTP monitor can alert; warn level below the limit.

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
```

## Tests / lint

```bash
make test    # fixture aggregation tests; no live Kafka needed
make vet
```

## Environment

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `KAFKA_BROKERS` | yes | — | comma-separated seed brokers, no scheme |
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
| `DATABASE_DSN` | yes | — | MariaDB DSN or `file:` SQLite path |
| `DATABASE_ENGINE` | no | inferred | `mariadb` \| `sqlite` |
| `BASE_PATH` | no | `/kafka` | must equal the chart `path` value |
| `LISTEN_ADDR` | no | `:8080` | non-root ports only in the container |
| `UI_TOKEN` | no | — | optional bearer auth for dashboard/APIs (health stays open) |
| `LOG_LEVEL` | no | `info` | slog level |

Secrets arrive via K8s `secretKeyRef` / chart `envFromSecret`; there is no
inline credential support. See the chart wiring section below.

## Database

Shares the Phoenix MariaDB (`phoenix` database), its own user and
`ext_kafka_usage_*` tables. Apply `deploy/grants.sql` after substituting the
user's password from your vault, then let the extension create its own
schema on startup (embedded migrations run before the first poll).

The store auto-appends `parseTime=true&multiStatements=true` when missing.
Chart integration (values.yaml example, port override, NetworkPolicy gotcha)
is documented in [`docs/chart-wiring.md`](docs/chart-wiring.md).