/*
03_demo.sql

Queries to demonstrate incremental refresh capabilities of the
dynamic table pipeline with sample data generation.
*/

USE ROLE lab_role;
USE DATABASE tasty_bytes_db;
USE WAREHOUSE tasty_bytes_wh;

-- Grab current state --> 248,201,269 records
SELECT COUNT(*) AS total_orders, MAX(order_ts) AS latest_order
FROM tasty_bytes_db.raw.order_header;

-- 673,655,465 records
SELECT COUNT(*) AS total_order_details FROM tasty_bytes_db.raw.order_detail;

-- Generate new demo orders (stored procedure generates both header and detail records)
CALL tasty_bytes_db.raw.generate_demo_orders(500);

-- Verify new data in raw tables
SELECT COUNT(*) AS total_orders FROM tasty_bytes_db.raw.order_header; -- +500 records
SELECT COUNT(*) AS total_order_details FROM tasty_bytes_db.raw.order_detail; --+1,397 records

-- Manually trigger incremental refresh across all dynamic tables
-- Tier 1 tables will refresh first
ALTER DYNAMIC TABLE tasty_bytes_db.analytics.orders_enriched REFRESH;
ALTER DYNAMIC TABLE tasty_bytes_db.analytics.order_items_enriched REFRESH;

-- Check refresh history for orders_enriched and order_items_enriched
SELECT name, refresh_action, state, refresh_start_time, refresh_trigger
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME => 'tasty_bytes_db.ANALYTICS.ORDERS_ENRICHED'
))
ORDER BY REFRESH_START_TIME DESC LIMIT 10;

SELECT name, refresh_action, state, refresh_start_time, refresh_trigger
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME => 'tasty_bytes_db.ANALYTICS.ORDER_ITEMS_ENRICHED'
))
ORDER BY REFRESH_START_TIME DESC LIMIT 10;

-- Tier 2 and 3 will refresh due to DOWNSTREAM lag. Manually trigger:
ALTER DYNAMIC TABLE tasty_bytes_db.analytics.order_fact REFRESH;
ALTER DYNAMIC TABLE tasty_bytes_db.analytics.daily_business_metrics REFRESH;
ALTER DYNAMIC TABLE tasty_bytes_db.analytics.product_performance_metrics REFRESH;

-- Check refresh history for order_fact
SELECT name, refresh_action, state, refresh_start_time
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME => 'tasty_bytes_db.ANALYTICS.ORDER_FACT'
))
ORDER BY REFRESH_START_TIME DESC LIMIT 10;

-- Check refresh history for daily_business_metrics
SELECT name, refresh_action, state, refresh_start_time
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME => 'tasty_bytes_db.ANALYTICS.DAILY_BUSINESS_METRICS'
))
ORDER BY REFRESH_START_TIME DESC LIMIT 10;

-- Check refresh history for daily_business_metrics
SELECT name, refresh_action, state, refresh_start_time
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME => 'tasty_bytes_db.ANALYTICS.PRODUCT_PERFORMANCE_METRICS'
))
ORDER BY REFRESH_START_TIME DESC LIMIT 10;

-- Updated metrics from today
SELECT order_date, total_orders, total_items_sold, total_revenue, total_profit
FROM tasty_bytes_db.analytics.daily_business_metrics
ORDER BY order_date DESC LIMIT 5;

-- Updated product performance
SELECT menu_item_name, item_category, total_units_sold, total_revenue, total_profit
FROM tasty_bytes_db.analytics.product_performance_metrics
ORDER BY total_revenue DESC
LIMIT 10;