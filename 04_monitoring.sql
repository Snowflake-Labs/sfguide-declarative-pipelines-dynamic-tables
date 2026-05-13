/*
================================================================================
CORTEX CODE PROMPT
================================================================================
Copy the prompt below into Cortex Code (Cmd+L in your Workspace).
CoCo will execute directly and show you the results.
================================================================================

Show me a monitoring dashboard for all dynamic tables in 
tasty_bytes_db.analytics: list each table with its scheduling state, target 
lag, and for the most recent refresh show the refresh type (incremental vs 
full), state, and duration in seconds.

================================================================================
EXPECTED OUTPUT
The SQL below is what Cortex Code should generate.
================================================================================
*/

USE ROLE lab_role;
USE DATABASE tasty_bytes_db;
USE WAREHOUSE tasty_bytes_wh;

SHOW DYNAMIC TABLES IN SCHEMA tasty_bytes_db.analytics;

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
