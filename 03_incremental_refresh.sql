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
Call tasty_bytes_db.raw.generate_demo_orders(500)

PROMPT 3:
Using tasty_bytes_wh, manually refresh all dynamic tables in
tasty_bytes_db.analytics in dependency order: first refresh orders_enriched
and order_items_enriched (tier 1), then order_fact (tier 2), then
daily_business_metrics and product_performance_metrics (tier 3).

PROMPT 4:
Query tasty_bytes_db.information_schema.dynamic_table_refresh_history() for each
of the 5 dynamic tables in tasty_bytes_db.analytics. Show the most recent
refresh_action, state, refresh_trigger, and duration in seconds for each.

PROMPT 5:
Query tasty_bytes_db.analytics.daily_business_metrics ordered by order_date
descending, limit 5. Then query tasty_bytes_db.analytics.product_performance_metrics
ordered by total_revenue descending, limit 10.

================================================================================
Reference SQL is available in the solutions branch.
================================================================================
*/
