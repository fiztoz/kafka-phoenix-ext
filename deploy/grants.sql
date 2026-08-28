-- kafka-phoenix-ext DB grants. Replace the placeholder with a randomly
-- generated value from your vault, then run once against Phoenix MariaDB.
-- Scoped strictly to this extension's tables: identifiers never conflict.
CREATE USER IF NOT EXISTS 'kafka_usage'@'%' IDENTIFIED BY '<generate-from-vault>';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, INDEX, ALTER
  ON phoenix.ext_kafka_usage_% TO 'kafka_usage'@'%';
FLUSH PRIVILEGES;
