-- MariaDB only. The runner skips this file on SQLite, whose default
-- BINARY collation is already case-sensitive.
--
-- Topic names are case-sensitive Kafka identifiers. A shared Phoenix
-- database defaults to utf8mb4_unicode_ci, which collapses Orders and
-- orders onto one primary-key row. utf8mb4_bin keeps them distinct.
--
-- Existing-data limitation: if both spellings were already written, the
-- case-insensitive key kept only one row. This migration cannot reconstruct
-- the lost spelling. Resolve that collision before upgrading if both names
-- still matter; the surviving row keeps whatever spelling was stored.
ALTER TABLE ext_kafka_usage_thresholds
  MODIFY COLUMN topic VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL;
ALTER TABLE ext_kafka_usage_topic_state
  MODIFY COLUMN topic VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL;
