CREATE TABLE IF NOT EXISTS ext_kafka_usage_thresholds (
  topic           VARCHAR(255) NOT NULL,
  threshold_bytes BIGINT NOT NULL,
  warn_bytes      BIGINT NULL,
  updated_at      TIMESTAMP NOT NULL,
  PRIMARY KEY (topic)
);

-- Durable last observation + hysteresis so a pod restart does not
-- forget a confirmed over-threshold or the previous good sample.
CREATE TABLE IF NOT EXISTS ext_kafka_usage_topic_state (
  topic           VARCHAR(255) NOT NULL,
  partitions      INT NOT NULL DEFAULT 0,
  storage_bytes   BIGINT NOT NULL DEFAULT 0,
  prev_bytes      BIGINT NOT NULL DEFAULT 0,
  polled_at       TIMESTAMP NOT NULL,
  prev_polled_at  TIMESTAMP NULL,
  over_streak     INT NOT NULL DEFAULT 0,
  confirmed_over  TINYINT(1) NOT NULL DEFAULT 0,
  last_error      TEXT NULL,
  PRIMARY KEY (topic)
);