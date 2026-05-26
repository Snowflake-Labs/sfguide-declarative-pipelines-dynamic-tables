/*
================================================================================
CORTEX CODE PROMPT
================================================================================
Copy the prompt below into Cortex Code.
================================================================================

Show a monitoring summary for all dynamic tables in tasty_bytes_db.analytics.
For each table, show its name, target_lag, scheduling state, and — from
tasty_bytes_db.information_schema.dynamic_table_refresh_history() — the most
recent refresh_action, state, and duration in seconds. Use a window function
to get only the latest refresh per table.

================================================================================
Reference SQL is available in the solutions branch.
================================================================================
*/
