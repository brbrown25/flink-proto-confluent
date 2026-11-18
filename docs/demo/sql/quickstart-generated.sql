-- The query half of sql/quickstart.sql, for the generated path: run it after sql/orders.sql, the
-- CREATE TABLE that bin/generate-ddl prints for the orders-value subject (see casts/quickstart.sh
-- and the README). sql/quickstart.sql runs the same query on its own with a hand-written table.
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';

SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer;
