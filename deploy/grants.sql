-- Least-privilege provisioning for kafka-phoenix-ext.
--
-- Run as an administrator. Substitute the account password from your secret
-- store for the REPLACE_BEFORE_RUN token before executing. Do not commit the
-- substituted file, and do not reuse Phoenix's application credentials.
--
-- The extension creates and uses tables only in the database selected by
-- DATABASE_DSN. Point that DSN at kafka_usage, not the Phoenix application
-- schema. Schema-level rights cover every table the process creates,
-- including ext_kafka_usage_schema_migrations and tables added by later
-- migrations — no per-table GRANT update is required when a migration adds
-- a table.
--
-- utf8mb4_bin is required: Kafka topic names are case-sensitive, and a
-- unicode_ci schema would collapse Orders and orders onto one row.
--
-- If a dedicated schema is impossible, do not grant every table in the
-- Phoenix application schema. Grant each current extension table explicitly
-- and repeat that GRANT before deploying a migration that creates another
-- table. Current tables:
--   ext_kafka_usage_schema_migrations
--   ext_kafka_usage_thresholds
--   ext_kafka_usage_topic_state
-- A table-prefix wildcard is not valid MariaDB GRANT syntax. Quoting it
-- grants one literal table name, not every matching table.

CREATE DATABASE IF NOT EXISTS kafka_usage CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE USER IF NOT EXISTS 'kafka_usage'@'%' IDENTIFIED BY 'REPLACE_BEFORE_RUN';

GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX
  ON kafka_usage.* TO 'kafka_usage'@'%';

FLUSH PRIVILEGES;
