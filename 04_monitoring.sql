/*
04_monitoring.sql

Quick reference queries for monitoring dynamic table operations
*/

USE ROLE lab_role;
USE DATABASE tasty_bytes_db;
USE WAREHOUSE tasty_bytes_wh;

-- Check all dynamic tables in the analytics schema
SHOW DYNAMIC TABLES IN SCHEMA tasty_bytes_db.analytics;

-- Summary of latest refresh operations across all dynamic tables
-- Shows refresh type (INCREMENTAL vs FULL) and duration
SELECT
  name,
  refresh_action,
  state,
  refresh_start_time,
  refresh_end_time,
  DATEDIFF('second', refresh_start_time, refresh_end_time) AS refresh_duration_seconds
FROM (
  SELECT name, refresh_action, state, refresh_start_time, refresh_end_time,
         ROW_NUMBER() OVER (PARTITION BY name ORDER BY refresh_start_time DESC) as rn
  FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
)
WHERE rn = 1
ORDER BY name;
