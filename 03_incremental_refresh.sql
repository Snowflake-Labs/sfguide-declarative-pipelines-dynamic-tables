/*
================================================================================
CORTEX CODE PROMPTS (Sequential)
================================================================================
Give Cortex Code the following prompts one at a time. Each prompt builds on the
previous step. CoCo will execute directly.
================================================================================

PROMPT 1:
How many rows are currently in tasty_bytes_db.raw.order_header and 
tasty_bytes_db.raw.order_detail?

PROMPT 2:
Call tasty_bytes_db.raw.generate_demo_orders with 500 rows

PROMPT 3:
Verify the new data: count rows again in order_header and order_detail. 
How many new rows were added?

PROMPT 4:
Manually refresh all dynamic tables in tasty_bytes_db.analytics in 
dependency order: first refresh orders_enriched and order_items_enriched 
(tier 1), then order_fact (tier 2), then daily_business_metrics and 
product_performance_metrics (tier 3).

PROMPT 5:
Show me the refresh history for all 5 dynamic tables in 
tasty_bytes_db.analytics. Was the latest refresh incremental or full? 
How long did each take?

PROMPT 6:
Show me the latest daily business metrics (top 5 most recent dates) and 
the top 10 products by revenue from the product_performance_metrics table.

================================================================================
EXPECTED OUTPUT
The SQL below shows the operations Cortex Code should execute. The key 
insight: after the manual refresh, refresh_action should show INCREMENTAL 
(not FULL) because only 500 new orders were processed.
================================================================================
*/

USE ROLE lab_role;
USE DATABASE tasty_bytes_db;
USE WAREHOUSE tasty_bytes_wh;

-- PROMPT 1: Check current state
SELECT COUNT(*) AS total_orders, MAX(order_ts) AS latest_order
FROM tasty_bytes_db.raw.order_header;

SELECT COUNT(*) AS total_order_details FROM tasty_bytes_db.raw.order_detail;

-- PROMPT 2: Generate new orders
CALL tasty_bytes_db.raw.generate_demo_orders(500);

-- PROMPT 3: Verify new data
SELECT COUNT(*) AS total_orders FROM tasty_bytes_db.raw.order_header;
SELECT COUNT(*) AS total_order_details FROM tasty_bytes_db.raw.order_detail;

-- PROMPT 4: Manually refresh all tiers
ALTER DYNAMIC TABLE tasty_bytes_db.analytics.orders_enriched REFRESH;
ALTER DYNAMIC TABLE tasty_bytes_db.analytics.order_items_enriched REFRESH;
ALTER DYNAMIC TABLE tasty_bytes_db.analytics.order_fact REFRESH;
ALTER DYNAMIC TABLE tasty_bytes_db.analytics.daily_business_metrics REFRESH;
ALTER DYNAMIC TABLE tasty_bytes_db.analytics.product_performance_metrics REFRESH;

-- PROMPT 5: Check refresh history
SELECT name, refresh_action, state, refresh_start_time, refresh_trigger
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME => 'tasty_bytes_db.ANALYTICS.ORDERS_ENRICHED'
))
ORDER BY REFRESH_START_TIME DESC LIMIT 5;

SELECT name, refresh_action, state, refresh_start_time, refresh_trigger
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME => 'tasty_bytes_db.ANALYTICS.ORDER_ITEMS_ENRICHED'
))
ORDER BY REFRESH_START_TIME DESC LIMIT 5;

SELECT name, refresh_action, state, refresh_start_time
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME => 'tasty_bytes_db.ANALYTICS.ORDER_FACT'
))
ORDER BY REFRESH_START_TIME DESC LIMIT 5;

SELECT name, refresh_action, state, refresh_start_time
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME => 'tasty_bytes_db.ANALYTICS.DAILY_BUSINESS_METRICS'
))
ORDER BY REFRESH_START_TIME DESC LIMIT 5;

SELECT name, refresh_action, state, refresh_start_time
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME => 'tasty_bytes_db.ANALYTICS.PRODUCT_PERFORMANCE_METRICS'
))
ORDER BY REFRESH_START_TIME DESC LIMIT 5;

-- PROMPT 6: View updated metrics
SELECT order_date, total_orders, total_items_sold, total_revenue, total_profit
FROM tasty_bytes_db.analytics.daily_business_metrics
ORDER BY order_date DESC LIMIT 5;

SELECT menu_item_name, item_category, total_units_sold, total_revenue, total_profit
FROM tasty_bytes_db.analytics.product_performance_metrics
ORDER BY total_revenue DESC
LIMIT 10;
