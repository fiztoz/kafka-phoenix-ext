# Chart wiring (uptime-phoenix values.yaml)

Minimal example. Secret material (SASL key, DB DSN) lives in two Secrets,
shown in the comments of `deploy/manifests/deployment.yaml` and in the
main `README.md` environment table — never in `values.yaml` literals:

```yaml
extensions:
  - id: kafka-usage          # DNS-1123 label; rendered as <release>-ext-kafka-usage
    title: Kafka             # sidebar label
    path: /kafka
    image: ghcr.io/fiztoz/kafka-phoenix-ext:0.1.0
    port: 8080               # override! chart default 80; non-root image binds 8080
    env:
      - name: KAFKA_BROKERS
        value: "kafka-0.kafka.svc:9092,kafka-1.kafka.svc:9092,kafka-2.kafka.svc:9092"
      - name: KAFKA_SASL_MECHANISM
        value: scram-sha-512
      - name: KAFKA_SASL_USERNAME
        value: kafka-usage   # service principal provisioned for this extension
      - name: KAFKA_TLS_ENABLED
        value: "true"
      - name: KAFKA_TLS_CA
        value: |
          ...inline PEM (or bake a configMap mount in a custom image)...
      - name: BASE_PATH
        value: /kafka
```

## Ops notes

- `port: 8080` is mandatory — the chart defaults ContainerPort/Service to 80
  and the distroless non-root image cannot bind 80. `readinessPath` can stay
  at its default; this image also serves probe paths at the root prefix.
- The chart's readiness probe (`/health/ready`) returns 503 until the first
  poll succeeds; keep an extra Phoenix monitor on `/health/live` if you want
  liveness signal distinct from data freshness.
- `networkPolicy.enabled: true` in Phoenix limits extension egress to
  80/443/4443/3306 + DNS — **no Kafka ports**. Apply
  `deploy/manifests/networkpolicy.yaml` (additive: 9092-9094 + 3306) or
  patch the chart.
- Cloudflare Tunnel users must add the `/kafka` path themselves.
- Standalone (non-chart) installs: `deploy/manifests/` has Deployment,
  Service, Ingress and NetworkPolicy to adapt.

## Phoenix monitors

Point Phoenix HTTP monitors at:

- `{base}/health/ready` — Kafka cluster unreachable
- `{base}/health/thresholds` — a topic is confirmed over its size threshold

Storage pressure is growth, not an outage: it must not flip any producer
heartbeat DOWN — use the thresholds endpoint for the alert body.