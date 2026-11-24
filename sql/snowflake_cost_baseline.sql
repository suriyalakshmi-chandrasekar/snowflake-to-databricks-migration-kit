-- Snowflake cost baseline: where the credits go, before you migrate anything.
-- Needs a role with access to SNOWFLAKE.ACCOUNT_USAGE (views lag by up to a few hours).
-- Use the results to order migration waves by cost, not by convenience.

-- 1. Credits per warehouse per month
SELECT warehouse_name,
       DATE_TRUNC('month', start_time)            AS month,
       ROUND(SUM(credits_used), 1)                AS credits,
       ROUND(SUM(credits_used_compute), 1)        AS compute_credits,
       ROUND(SUM(credits_used_cloud_services), 1) AS cloud_services_credits
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= DATEADD(month, -3, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY month DESC, credits DESC;

-- 2. Warehouses that (almost) never suspend: billed in nearly every hour.
--    Frequent polling queries are a common cause: auto-suspend never gets a quiet window.
SELECT warehouse_name,
       COUNT(DISTINCT DATE_TRUNC('hour', start_time))             AS billed_hours,
       DATEDIFF(hour, MIN(start_time), MAX(start_time)) + 1       AS hours_in_window,
       ROUND(billed_hours / NULLIF(hours_in_window, 0) * 100, 1)  AS pct_hours_billed
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= DATEADD(day, -14, CURRENT_TIMESTAMP())
  AND credits_used > 0
GROUP BY 1
HAVING pct_hours_billed > 90
ORDER BY pct_hours_billed DESC;

-- 3. Query families: the same query with different literals, grouped by parameterized hash
SELECT warehouse_name,
       query_parameterized_hash,
       ANY_VALUE(LEFT(query_text, 200))                   AS example,
       COUNT(*)                                           AS runs,
       ROUND(SUM(total_elapsed_time) / 1000 / 3600, 2)    AS total_hours,
       ROUND(MEDIAN(total_elapsed_time) / 1000, 2)        AS median_seconds
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(day, -14, CURRENT_TIMESTAMP())
  AND warehouse_name IS NOT NULL
  AND execution_status = 'SUCCESS'
GROUP BY 1, 2
ORDER BY total_hours DESC
LIMIT 50;

-- 4. Polling suspects: small queries that run very often
SELECT warehouse_name,
       query_parameterized_hash,
       COUNT(*)                                  AS runs,
       ROUND(COUNT(*) / (14 * 24 * 60.0), 2)     AS runs_per_minute,
       ROUND(AVG(total_elapsed_time), 0)         AS avg_ms
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(day, -14, CURRENT_TIMESTAMP())
GROUP BY 1, 2
HAVING runs_per_minute >= 0.5 AND avg_ms < 1000
ORDER BY runs DESC;
