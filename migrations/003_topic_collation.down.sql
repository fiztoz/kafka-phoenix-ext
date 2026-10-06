-- Reverts topic keys to a case-insensitive collation. Do not apply this on a
-- database that already stores case-distinct topic names; the primary key
-- would collide. The runner does not apply down migrations.
ALTER TABLE ext_kafka_usage_thresholds
  MODIFY COLUMN topic VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL;
ALTER TABLE ext_kafka_usage_topic_state
  MODIFY COLUMN topic VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL;
