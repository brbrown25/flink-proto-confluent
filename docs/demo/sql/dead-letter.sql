SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';

CREATE TABLE events (
  order_id STRING,
  customer STRING,
  amount   DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'docs-demo-dlq',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'value.format' = 'proto-confluent',
  'value.proto-confluent.url' = 'http://schema-registry:8081',
  'value.proto-confluent.topic' = 'events',
  'value.proto-confluent.is_key' = 'false',
  'value.proto-confluent.on-deserialize-error' = 'skip',
  'value.proto-confluent.dead-letter-topic' = 'events-dlq',
  'value.proto-confluent.dead-letter.properties' = 'bootstrap.servers:''kafka:9092'''
);

SELECT * FROM events;
