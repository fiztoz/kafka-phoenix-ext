-- MariaDB. Scope every durable row to a Kafka cluster so one shared schema
-- can back several polled clusters. Existing rows belong to the cluster the
-- pre-migration install was polling, which the app names "default".
--
-- The primary key has to become (cluster, topic) for both tables: two
-- clusters may legitimately carry a topic with the same name.
-- MariaDB DDL implicitly commits. Each ALTER is replay-safe even if the
-- process stopped before the migration version was recorded, or an earlier
-- attempt only added a column. Replacing an already-correct PK is harmless.
ALTER TABLE ext_kafka_usage_topic_state
  ADD COLUMN IF NOT EXISTS cluster VARCHAR(64) NOT NULL DEFAULT 'default' AFTER topic,
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (cluster, topic);

ALTER TABLE ext_kafka_usage_thresholds
  ADD COLUMN IF NOT EXISTS cluster VARCHAR(64) NOT NULL DEFAULT 'default' AFTER topic,
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (cluster, topic);