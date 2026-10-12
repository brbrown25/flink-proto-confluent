SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';

CREATE TABLE orders (
  order_id STRING,
  customer STRING,
  amount   DOUBLE,
  currency STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'docs-demo-evolution',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'value.format' = 'proto-confluent',
  'value.proto-confluent.url' = 'http://schema-registry:8081',
  'value.proto-confluent.topic' = 'orders',
  'value.proto-confluent.is_key' = 'false'
);

SELECT * FROM orders;
