# Chart wiring (uptime-phoenix values.yaml)

Minimal example. Secret material lives in the two Secrets the standalone
Deployment comments name. The chart does not mount those Secrets unless the
values below say so. `envFromSecret` is not used here: that injects every
key as an environment variable, and the SASL secret key is `sasl-password`,
not `KAFKA_SASL_PASSWORD`.

```yaml
extensions:
  - id: kafka-usage          # DNS-1123 label; rendered as <release>-ext-kafka-usage
    title: Kafka             # sidebar label
    path: /kafka
    image: ghcr.io/fiztoz/kafka-phoenix-ext:0.2.3
    port: 8080               # override! chart default 80; non-root image binds 8080
    readinessPath: /health/live
    database:
      secretName: kafka-phoenix-ext-db
      secretKey: dsn
    env:
      # One cluster. Add KAFKA_CLUSTERS + per-cluster broker lists to poll
      # several; see "Several clusters" below.
      - name: KAFKA_BROKERS
        value: "kafka-0.kafka.svc:9092,kafka-1.kafka.svc:9092,kafka-2.kafka.svc:9092"
      - name: KAFKA_SASL_MECHANISM
        value: scram-sha-512
      - name: KAFKA_SASL_USERNAME
        value: kafka-usage   # service principal provisioned for this extension
      - name: KAFKA_SASL_PASSWORD
        valueFrom:
          secretKeyRef:
            name: kafka-phoenix-ext
            key: sasl-password
      - name: KAFKA_TLS_ENABLED
        value: "true"
      - name: KAFKA_TLS_CA
        value: |
          ...inline PEM (or bake a configMap mount in a custom image)...
      - name: BASE_PATH
        value: /kafka
```

Create those Secrets before install. `kafka-phoenix-ext-db` key `dsn` is a
MariaDB DSN whose database is `kafka_usage` (see `deploy/grants.sql`).
`kafka-phoenix-ext` key `sasl-password` is the SASL password. Do not put
either value in `values.yaml`.

## Ops notes

- `port: 8080` is mandatory — the chart defaults ContainerPort/Service to 80
  and the distroless non-root image cannot bind 80.
- `readinessPath: /health/live` is mandatory. The chart default
  `/health/ready` is the Kafka dependency monitor and returns 503 while the
  cluster is unreachable. Kubernetes then removes the only pod from Service
  backends, so the cached dashboard and an independent `/health/live` monitor
  both become unreachable. The process serves `/health/live` at the container
  root as well as under `BASE_PATH`. Keep a Phoenix HTTP monitor on
  `/health/ready` for Kafka itself; do not use that path as the pod probe.
- `networkPolicy.enabled: true` in Phoenix limits extension egress to
  80/443/4443/3306 + DNS — **no Kafka ports**. The chart policy selects
  `app.kubernetes.io/name: uptime-phoenix-ext`, the release instance, and
  `app.kubernetes.io/component: extension-kafka-usage`. Apply
  `deploy/manifests/networkpolicy-chart.yaml` and set
  `app.kubernetes.io/instance` to the Helm release name (`phoenix` in that
  file). A `nameOverride` changes the name label to `<override>-ext`.
  Standalone installs use `deploy/manifests/networkpolicy.yaml` instead;
  that policy selects `app.kubernetes.io/name: kafka-phoenix-ext` and allows
  DNS. Do not apply the chart selector to standalone pods.
- Cloudflare Tunnel users must add the `/kafka` path themselves.
- Standalone (non-chart) installs: `deploy/manifests/` has Deployment,
  Service, Ingress and NetworkPolicy to adapt. The Deployment readiness
  probe is `/kafka/health/live`, matching liveness, not `/health/ready`.

## Phoenix monitors

Point Phoenix HTTP monitors at:

- `{base}/health/ready` — Kafka cluster unreachable
- `{base}/health/thresholds` — a topic is confirmed over its size threshold
- `{base}/health/live` — process liveness, independent of Kafka

Storage pressure is growth, not an outage: it must not flip any producer
heartbeat DOWN — use the thresholds endpoint for the alert body.

## Several clusters

One extension instance can poll several Kafka clusters: declare the ids and give
each its own broker list. Per-cluster values fall back to the shared
`KAFKA_*` block, so a shared principal only costs two extra lines. The names
`harbor` and `summit` in this fresh-install example are fictional.

**Existing installations:** keep the original cluster as `default`, rename
its `KAFKA_BROKERS` variable to `KAFKA_DEFAULT_BROKERS` (same value), remove
`KAFKA_BROKERS`, and use `KAFKA_CLUSTERS=default,summit`. Keep the original
SASL/TLS/filter settings and set `KAFKA_LEGACY_CLUSTER=default`. Back up the
database first; see the [full upgrade procedure](../README.md#upgrading-an-existing-single-cluster-installation).
This keeps migrated thresholds and hysteresis active. The example below is
for an empty database, not a rename of an existing installation.

```yaml
    env:
      - name: KAFKA_CLUSTERS
        value: "harbor,summit"
      - name: KAFKA_HARBOR_BROKERS
        value: "harbor-kafka-0.harbor.svc:9092,harbor-kafka-1.harbor.svc:9092"
      - name: KAFKA_SUMMIT_BROKERS
        value: "summit-kafka-0.summit.svc:9092"
      # Shared defaults for both clusters.
      - name: KAFKA_SASL_MECHANISM
        value: scram-sha-512
      - name: KAFKA_SASL_USERNAME
        value: kafka-usage
      - name: KAFKA_SASL_PASSWORD
        valueFrom:
          secretKeyRef:
            name: kafka-phoenix-ext
            key: sasl-password
      # Per-cluster override: one Secret key per cluster when the principals
      # differ. Keys are `sasl-password-<cluster>` by convention.
      - name: KAFKA_SUMMIT_SASL_PASSWORD
        valueFrom:
          secretKeyRef:
            name: kafka-phoenix-ext
            key: sasl-password-summit
```

Notes for the chart:

- Keep `readinessPath: /health/live`. It is process liveness, not any one
  cluster's health.
- Point Phoenix monitors at the cluster they are about:
  `{base}/health/c/harbor/ready`, `{base}/health/c/harbor/thresholds`, ... The
  unprefixed `{base}/health/ready` stays bound to `KAFKA_LEGACY_CLUSTER`
  (`default` when configured, otherwise the first declared cluster).
  `KAFKA_LEGACY_CLUSTER` does not rename stored data; existing installations
  must retain `default` as described above. Startup refuses to silently leave
  populated `default` state or thresholds unconfigured.
- `{base}/health/clusters` fails while any configured cluster cannot be
  polled — one monitor for "the extension can see everything".
- The cluster switcher and the overview page are same-origin links, so the
  existing `BASE_PATH` Ingress rule and the UI session cookie cover them; no
  extra Ingress path is needed.
- `deploy/manifests/networkpolicy-chart.yaml` allows Kafka ports 9092/9094.
  Brokers in a **different** cluster/network need their own egress CIDR rule;
  the shipped policy does not know your second Kafka subnet.
- Per-cluster thresholds are separate rows. Setting `orders` on `harbor` never
  affects `orders` on `summit`.
