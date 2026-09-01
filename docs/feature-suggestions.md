# Feature suggestions

Backlog only. This extension stays a **read-only Phoenix sidecar** for Kafka
ops health: poller → snapshot/store → iframe UI + HTTP monitors. It is not
AKHQ / CMAK / Redpanda Console. Suggestions below reuse that shape (admin
API, optional extra ACLs, hysteresis, `/health/*` 503 for Phoenix).

The **Lag** column on `/topic/{name}` today is **replica log lag** from
`DescribeLogDirs` (`OffsetLag`), not consumer-group lag.

---

## Status (2026-09)

Shipped (no extra ACLs needed):

- **#2** `/health/replicas` with 2-poll hysteresis; wallboard/dashboard badge it.
- **#3** `/health/brokers` + hottest-broker share on dashboard/wallboard
  (`KAFKA_BROKER_SKEW_PCT`, `KAFKA_BROKER_MAX_BYTES`).
- **#4** growth thresholds (`growth_bytes_per_hour` on the thresholds form/API),
  time-to-limit forecast on dashboard/wallboard/topic, `/health/growth`.
- **#6** partition skew (max/avg leader size) on the topic page and as a
  sortable dashboard column.
- **#7 (lite)** growth now uses an in-memory rolling window (last 8 polls) so
  one compaction dip does not flip alerts; long history still belongs to #10.
- **#8** failed polls are classified (auth / timeout / network); the dashboard
  banner shows the required ACLs on auth failures and `/health/ready` never
  reports "unreachable" for an ACL problem.
- **#9** `/broker/{id}` drill-down (hosted topics, largest led partitions).
- **#12** `/api/brokers` plus growth/replica/skew fields in the topic JSON.

Deferred (needs new ACLs, protocol work or dependencies, per the staging
below): **#1** consumer groups, **#5** retention configs, **#10** Prometheus
`/metrics`, **#11** mTLS / OAUTHBEARER.

---

## 1. Consumer groups and lag (requested)

**Problem.** Operators cannot see who is consuming, whether a group is empty
or stuck, or how far committed offsets sit behind the log end. Disk size of
`__consumer_offsets` (via `KAFKA_INCLUDE_INTERNAL`) is not that.

**What to show**

- Groups: id, state (`Stable` / `Empty` / `Dead` / `PreparingRebalance`),
  member count, protocol, coordinator.
- Per group × topic × partition: committed offset, high watermark,
  **consumer lag** (watermark − committed). Label it “consumer lag” so it
  is not confused with replica `OffsetLag`.
- Topic page: which groups read this topic, and total lag.
- Wallboard: groups over a lag threshold, empty groups that still have
  lag (abandoned).

**How (admin protocol only)**

1. `ListGroups` / `DescribeGroups`
2. `OffsetFetch` for committed offsets
3. `ListOffsets` (latest) for watermarks
4. lag = watermark − committed (clamp at 0)

**ACLs (new; current principal does not need these)**

| Resource | Operation | Why |
|---|---|---|
| `GROUP` `*` (or prefix) | Describe | list / describe groups |
| `GROUP` `*` | Read | `OffsetFetch` on many clusters |
| `TOPIC` (already wanted) | Describe + Read | watermarks via `ListOffsets` |

No `Write`, `Create`, `Delete`, `TransactionalId`. Read-only: never
reset offsets, never delete groups.

**Phoenix monitors** (same hysteresis as size: 2 consecutive polls)

- `GET /health/lag` — 503 when any watched group is confirmed over lag
- `GET /health/lag/{group}` — per-group
- Operator-set lag thresholds (messages or estimated bytes) in store,
  same form pattern as size thresholds

**Config**

- `KAFKA_GROUP_FILTER` regexp (default `.*`, hide Connect/internal if wanted)
- Feature stays optional: if GROUP ACLs are missing, storage polling
  still works; group page shows a clear “unauthorized” banner instead of
  failing `/health/ready`

**Out of scope for v1 of this feature:** live member hostnames as a
security surface in the public iframe, offset reset, consume/produce
console.

---

## 2. Replica health as a Phoenix monitor

**Already collected:** `UnderRep`, `Offline` per topic/partition.

**Gap:** no `/health/*` for it, so Phoenix cannot alert without scraping
the HTML/JSON.

- `GET /health/replicas` — 503 when any partition stays under-replicated
  or offline across `confirmSamples`
- Wallboard already badges under-rep; keep that as the visual

No extra ACLs (`Metadata` is enough).

---

## 3. Broker disk skew / per-broker threshold

**Already collected:** per-broker bytes and replica-copy count.

**Gap:** no alert when one broker holds a disproportionate share, or when
a broker’s reported log dirs exceed a limit (before the disk fills).

- Dashboard: max/min broker ratio, “hot” broker
- Threshold: absolute bytes **or** “any broker > N% of cluster total”
- `GET /health/brokers` — 503 on confirmed skew/over

Uses existing `DescribeLogDirs`. Optional later: `DescribeLogDirs` per
mount path (multiple log dirs on one broker).

---

## 4. Growth-rate and time-to-limit

**Already collected:** `GrowthPerHour` from last two good samples.

**Gap:** alerts are absolute size only. A quiet 800 GiB topic is fine; a
20 GiB topic growing 4 GiB/h is not.

- Per-topic growth threshold (bytes/hour)
- Forecast: hours until size threshold at current growth (hide when
  growth ≤ 0)
- `GET /health/growth` — 503 when confirmed over growth limit
- Wallboard tile: “~Xh to limit”

Needs a slightly longer baseline than 2 samples (e.g. last 4 polls) so a
single compaction does not trip it.

---

## 5. Retention and cleanup policy (DescribeConfigs)

**Problem.** Size without retention is hard to act on: compact topics
behave unlike delete topics; `retention.bytes=-1` + `retention.ms=-1`
is unbounded.

**What to show** (topic page + optional column)

- `cleanup.policy`, `retention.ms`, `retention.bytes`, `min.insync.replicas`
- Badge: **unbounded retention**
- Compare `retention.bytes` to current `leader_bytes` / `storage_bytes`

**ACL:** `TOPIC` **DescribeConfigs** (not granted today). Degrade
gracefully if missing.

Still read-only: no `AlterConfigs`.

---

## 6. Partition size skew

**Already collected:** per-partition leader size on the topic page.

**Gap:** no cluster-level “this topic has one huge partition” signal
(hot key / bad partitioner), which drives disk imbalance.

- Topic page: max/median partition size, Gini or simple max/avg
- Dashboard sort: “most skewed”
- Optional warn when max partition > N× average

No extra APIs.

---

## 7. Storage history (more than one previous sample)

**Today:** `prev_bytes` / `prev_polled_at` only (growth + hysteresis).

**Suggestion:** keep a **short** in-process or SQL window (last 4–16
polls) so growth / time-to-limit is not fooled by one compaction dip.
That is for the poller and the topic page sparkline.

Long retention (days/weeks of graphs) belongs on **Prometheus (#10)**,
not MariaDB. A growing `ext_kafka_usage_topic_samples` table duplicates
what a scrape already stores more efficiently.

---

## 8. Degraded-auth and ACL status on the UI

**Problem.** `CLUSTER_AUTHORIZATION_FAILED` on `DescribeLogDirs` looks like
“Kafka is down” (`/health/ready` 503) even though SASL worked.

**Suggestion**

- Classify last error: auth vs network vs timeout vs partial brokers
- Banner: “authenticated, missing CLUSTER Describe/DescribeLogDirs”
- Document required ACLs next to the banner (CLUSTER Describe,
  TOPIC Describe; groups later)
- Partial success (`ShardErrors` with some brokers) should keep
  `poll_ok=true` with `BrokerErrors` prominent — already the Describe
  contract; make it visible on the dashboard if it is easy to miss

`/health/ready` should stay 503 when **no** usable sample exists; an
auth failure is still “not ready” for storage data, but the message
must not say “unreachable”.

---

## 9. Broker / log-dir drill-down

**Today:** broker chips on the home page (id, host, bytes, replica count).

**Suggestion:** `/broker/{id}` — log dirs, topics hosted, largest
partitions on that broker. Helps “why is broker 3 fat?” without SSH.

Same `DescribeLogDirs` + `Metadata` payload; mostly HTTP/UI.

---

## 10. Optional Prometheus `/metrics`

Phoenix monitors stay the pager (HTTP 200/503). `/metrics` is for graphs
and recording rules, not a second Kafka poll.

**Efficiency rule:** scrape the **last in-memory snapshot only**. Never
call `DescribeLogDirs` from the scrape handler. Kafka stays on
`KAFKA_POLL_INTERVAL` (default 15m); Prometheus just reads what the
poller already paid for. That is cheaper than scraping `/api/topics`
JSON, cheaper than a SQL sample table for long history (#7), and far
cheaper than kafka_exporter hitting the cluster on every scrape.

**How to implement (do this, not GaugeVec.Set in the poller)**

Use `prometheus/client_golang` as a **custom `Collector`**: `Collect()`
copies `Poller.Snapshot()` and emits gauges. Deleted topics disappear
on the next scrape (no stale series). `GaugeVec.Set` in the poll loop
leaks labelsets forever unless you `Reset()`, which is the inefficient
pattern.

Do not hand-roll exposition text. The client handles escaping, `TYPE` /
`HELP`, and content negotiation.

**Cardinality (default = cheap)**

| Series | Labels | Default |
|---|---|---|
| topic storage / leader / growth / under-rep / offline | `topic` | on |
| broker storage / replica copies | `broker_id` (not `host`) | on |
| process: `poll_ok`, `polled_at_timestamp_seconds`, `describe_duration_seconds` | — | on |
| partition size / replica lag | `topic`, `partition` | **off** |
| consumer lag | `group`, `topic` (sum) | later, opt-in |
| consumer lag per partition | `group`, `topic`, `partition` | **never default** |

Topic × partition × group is how Kafka exporters melt Prometheus. Keep
partition/group series behind `KAFKA_METRICS_DETAIL=topic\|partition`
(or similar). `host` as a label duplicates `broker_id` and churns if
DNS changes.

**Scrape interval:** match poll cadence (1–15m), not Prometheus’s 15s
default. Repeating the same snapshot every 15s only writes TSDB
duplicates. `polled_at_timestamp_seconds` tells Grafana the point is
stale.

**Metric types:** gauges for sizes (compaction makes them go down).
Counter only for poll attempts / failures. No histograms of topic size.

**Auth:** token-guard like `/api/*`. Health stays open and is what
Phoenix probes.

**Not the primary alert path.** Recording rules are fine; paging still
goes through `/health/*` so this image works without a Prometheus
stack.

---

## 11. Extra SASL / TLS modes

**Today:** `none` | `plain` | `scram-sha-256` | `scram-sha-512`, optional
TLS with CA file/inline. No client certs, no OAUTHBEARER.

Worth adding when a cluster requires it:

- mTLS (`KAFKA_TLS_CERT` / `KAFKA_TLS_KEY`)
- OAUTHBEARER (client-credentials) for cloud Kafka

Not a product feature for the iframe; just reachability.

---

## 12. JSON completeness for automation

`GET /api/topics` is a thin snapshot. Useful additions:

- `poll_ok`, `last_error`, `polled_at` at the top level (some of this
  may already exist — keep a stable envelope)
- `under_replicated`, `offline`, `growth_per_hour` on every topic
- `GET /api/brokers`
- Later: `GET /api/groups`

Lets Phoenix stay the pager while something else graphs.

---

## Explicit non-goals

Leave these to a real Kafka UI or the control plane:

- Produce / consume messages
- Create / delete / alter topics
- ACL editing
- Offset reset, group delete
- Reassign partitions / preferred-leader election
- Schema Registry, Connect, ksqlDB
- JMX, broker OS disk (`df`), JVM heap

If a suggestion needs a write API or ClusterAction, it does not belong
here.

---

## Suggested order

| Order | Feature | Extra Kafka ACL | Phoenix monitor |
|---|---|---|---|
| 1 | Auth-error clarity (#8) | — | better ready text |
| 2 | Replica health (#2) | — | `/health/replicas` |
| 3 | Growth + time-to-limit (#4) | — | `/health/growth` |
| 4 | Broker skew (#3) | — | `/health/brokers` |
| 5 | Partition skew (#6) | — | optional |
| 6 | **Consumer groups + lag (#1)** | GROUP Describe/Read | `/health/lag` |
| 7 | History samples (#7) | — | better growth |
| 8 | Retention configs (#5) | TOPIC DescribeConfigs | unbounded badge |
| 9 | Broker drill-down (#9) | — | — |
| 10 | `/metrics` + richer JSON (#10, #12) | — | scrape optional |
| 11 | mTLS / OAUTHBEARER (#11) | — | — |

#1 is the largest (new APIs, ACLs, store, UI). Ship the storage-side
health gaps first so the existing poller earns more Phoenix monitors
without widening the principal.
