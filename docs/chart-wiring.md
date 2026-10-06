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
