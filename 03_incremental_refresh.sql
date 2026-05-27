/*
================================================================================
CORTEX CODE PROMPTS (Sequential)
================================================================================
Give Cortex Code the following prompts one at a time.
================================================================================

PROMPT 1:
How many rows are currently in tasty_bytes_db.raw.order_header and
tasty_bytes_db.raw.order_detail?

PROMPT 2:
Call tasty_bytes_db.raw.generate_demo_orders(1000)

PROMPT 3:
Refresh daily_business_metrics and product_performance_metrics in
tasty_bytes_db.analytics.

Note: Only trigger the top-tier tables — Snowflake automatically cascades
through Tier 2 and Tier 1 first due to the DOWNSTREAM dependency model.

PROMPT 4:
Query tasty_bytes_db.information_schema.dynamic_table_refresh_history() for each
of the 5 dynamic tables in tasty_bytes_db.analytics. Show the most recent
refresh_action, state, refresh_trigger, and duration in seconds for each.

Discussion: Notice that product_performance_metrics shows a FULL refresh.
Why? This table aggregates across all products — when a new order arrives for
an existing product, the aggregate row must be updated. Does a full rewrite
make sense here?

PROMPT 5:
Query tasty_bytes_db.analytics.daily_business_metrics ordered by order_date
descending, limit 5. Then query tasty_bytes_db.analytics.product_performance_metrics
ordered by total_revenue descending, limit 10.

================================================================================
Reference SQL is available in the solutions branch.
================================================================================
*/
