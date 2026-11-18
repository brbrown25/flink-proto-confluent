-- Runs after sql/orders.sql, the CREATE TABLE that bin/generate-ddl prints for the orders-value
-- subject (see casts/quickstart.sh and the README).
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';

SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer;
