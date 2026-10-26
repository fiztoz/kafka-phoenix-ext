-- SQLite. Same change as 004, expressed as a table rebuild: SQLite cannot
-- drop or alter a PRIMARY KEY in place. The runner executes every statement
-- and the version record inside one transaction; failures roll it all back.
--
-- Existing rows keep cluster='default', the id the pre-migration install used.
CREATE TABLE ext_kafka_usage_topic_state_rebuild (
  cluster         VARCHAR(64) NOT NULL DEFAULT 'default',
  topic           VARCHAR(255) NOT NULL,
  partitions      INT NOT NULL DEFAULT 0,
  storage_bytes   BIGINT NOT NULL DEFAULT 0,
  prev_bytes      BIGINT NOT NULL DEFAULT 0,
  polled_at       TIMESTAMP NOT NULL,
  prev_polled_at  TIMESTAMP NULL,
  over_streak     INT NOT NULL DEFAULT 0,
  confirmed_over  TINYINT(1) NOT NULL DEFAULT 0,
  last_error      TEXT NULL,
  PRIMARY KEY (cluster, topic)
);

INSERT INTO ext_kafka_usage_topic_state_rebuild
  (cluster, topic, partitions, storage_bytes, prev_bytes, polled_at,
   prev_polled_at, over_streak, confirmed_over, last_error)
SELECT 'default', topic, partitions, storage_bytes, prev_bytes, polled_at,
       prev_polled_at, over_streak, confirmed_over, last_error
FROM ext_kafka_usage_topic_state;

DROP TABLE ext_kafka_usage_topic_state;

ALTER TABLE ext_kafka_usage_topic_state_rebuild RENAME TO ext_kafka_usage_topic_state;

CREATE TABLE ext_kafka_usage_thresholds_rebuild (
  cluster               VARCHAR(64) NOT NULL DEFAULT 'default',
  topic                 VARCHAR(255) NOT NULL,
  threshold_bytes       BIGINT NOT NULL,
  warn_bytes            BIGINT NULL,
  growth_bytes_per_hour BIGINT NULL,
  updated_at            TIMESTAMP NOT NULL,
  PRIMARY KEY (cluster, topic)
);

INSERT INTO ext_kafka_usage_thresholds_rebuild
  (cluster, topic, threshold_bytes, warn_bytes, growth_bytes_per_hour, updated_at)
SELECT 'default', topic, threshold_bytes, warn_bytes, growth_bytes_per_hour, updated_at
FROM ext_kafka_usage_thresholds;

DROP TABLE ext_kafka_usage_thresholds;

ALTER TABLE ext_kafka_usage_thresholds_rebuild RENAME TO ext_kafka_usage_thresholds;