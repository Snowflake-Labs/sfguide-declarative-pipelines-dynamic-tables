/*
01_dynamic_tables.sql

Creates a 3-tier declarative pipeline using Snowflake Dynamic Tables
with incremental refresh support across all tiers.
*/

USE ROLE lab_role;
USE DATABASE tasty_bytes_db;
USE WAREHOUSE tasty_bytes_wh;

/*
Tier 1: Raw data enrichment - create ORDERS_ENRICHED and ORDER_ITEMS_ENRICHED tables
*/

-- ORDERS_ENRICHED: Orders enriched with temporal and financial metrics
CREATE OR REPLACE DYNAMIC TABLE tasty_bytes_db.analytics.orders_enriched
  TARGET_LAG = '12 hours'
  WAREHOUSE = tasty_bytes_wh
  AS
SELECT
  -- Order identifiers
  order_id,
  truck_id,
  customer_id,
  order_channel,
  -- Temporal dimensions
  order_ts AS order_timestamp,
  DATE(order_ts) AS order_date,
  DAYNAME(order_ts) AS day_name,
  HOUR(order_ts) AS order_hour,
  -- Financial metrics
  order_amount,
  order_total,
  TRY_TO_NUMBER(order_discount_amount, 10, 2) AS order_discount_amount,
  -- Simple discount flag
  CASE
    WHEN discount_id IS NOT NULL AND discount_id != '' THEN TRUE
    ELSE FALSE
  END AS has_discount
FROM tasty_bytes_db.raw.order_header
WHERE order_id IS NOT NULL
  AND order_ts IS NOT NULL;


-- ORDER_ITEMS_ENRICHED: Enriched order items with product details and profit calculations
CREATE OR REPLACE DYNAMIC TABLE tasty_bytes_db.analytics.order_items_enriched
  TARGET_LAG = '12 hours'
  WAREHOUSE = tasty_bytes_wh
  AS
SELECT
  -- Order detail identifiers
  od.order_detail_id,
  od.order_id,
  od.line_number,
  -- Product information
  od.menu_item_id,
  m.menu_item_name,
  m.item_category,
  m.item_subcategory,
  m.truck_brand_name,
  m.menu_type,
  -- Quantity and pricing
  od.quantity,
  od.unit_price,
  od.price AS line_total,
  m.cost_of_goods_usd,
  m.sale_price_usd,
  -- Profit calculations
  (od.unit_price - m.cost_of_goods_usd) AS unit_profit,
  (od.unit_price - m.cost_of_goods_usd) * od.quantity AS line_profit,
  CASE
    WHEN od.unit_price > 0 THEN
      ROUND(((od.unit_price - m.cost_of_goods_usd) / od.unit_price) * 100, 2)
    ELSE 0
  END AS profit_margin_pct,
  -- Discount information
  TRY_TO_NUMBER(od.order_item_discount_amount, 10, 2) AS line_discount_amount,
  CASE
    WHEN od.discount_id IS NOT NULL AND od.discount_id != '' THEN TRUE
    ELSE FALSE
  END AS has_discount
FROM tasty_bytes_db.raw.order_detail od
INNER JOIN tasty_bytes_db.raw.menu m
  ON od.menu_item_id = m.menu_item_id
WHERE od.order_id IS NOT NULL
  AND od.menu_item_id IS NOT NULL;


/*
Tier 2: Create ORDER_FACT table joining header and line items
*/

-- ORDER_FACT: Integrated order and line item data
CREATE OR REPLACE DYNAMIC TABLE tasty_bytes_db.analytics.order_fact
  TARGET_LAG = 'DOWNSTREAM' -- Checks upstream tables (tier 1) for changes then refreshes
  WAREHOUSE = tasty_bytes_wh
  AS
SELECT
  -- Order header fields
  o.order_id,
  o.truck_id,
  o.customer_id,
  o.order_channel,
  o.order_timestamp,
  o.order_date,
  o.day_name,
  o.order_hour,
  o.order_amount,
  o.order_total,
  o.order_discount_amount AS order_level_discount,
  o.has_discount AS order_has_discount,
  -- Order line item fields
  oi.order_detail_id,
  oi.line_number,
  oi.menu_item_id,
  oi.menu_item_name,
  oi.item_category,
  oi.item_subcategory,
  oi.truck_brand_name,
  oi.menu_type,
  oi.quantity,
  oi.unit_price,
  oi.line_total,
  oi.cost_of_goods_usd,
  oi.sale_price_usd,
  oi.unit_profit,
  oi.line_profit,
  oi.profit_margin_pct,
  oi.line_discount_amount,
  oi.has_discount AS line_has_discount
FROM tasty_bytes_db.analytics.orders_enriched o
INNER JOIN tasty_bytes_db.analytics.order_items_enriched oi
  ON o.order_id = oi.order_id;

/*
Tier 3: Aggregated metrics - Create DAILY_BUSINESS_METRICS and PRODUCT_PERFORMANCE_METRICS tables
*/

-- DAILY_BUSINESS_METRICS: Daily business metrics aggregated from ORDER_FACT
CREATE OR REPLACE DYNAMIC TABLE tasty_bytes_db.analytics.daily_business_metrics
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = tasty_bytes_wh
  AS
SELECT
  order_date,
  day_name,
  -- Volume metrics
  COUNT(DISTINCT order_id) AS total_orders,
  COUNT(DISTINCT truck_id) AS active_trucks,
  COUNT(DISTINCT customer_id) AS unique_customers,
  SUM(quantity) AS total_items_sold,
  -- Revenue metrics
  SUM(order_total) AS total_revenue,
  ROUND(AVG(order_total), 2) AS avg_order_value,
  SUM(line_total) AS total_line_item_revenue,
  -- Profit metrics
  SUM(line_profit) AS total_profit,
  ROUND(AVG(profit_margin_pct), 2) AS avg_profit_margin_pct,
  -- Discount metrics
  SUM(CASE WHEN order_has_discount THEN 1 ELSE 0 END) AS orders_with_discount,
  SUM(order_level_discount) AS total_order_discount_amount,
  SUM(line_discount_amount) AS total_line_discount_amount
FROM tasty_bytes_db.analytics.order_fact
GROUP BY order_date, day_name;

-- PRODUCT_PERFORMANCE_METRICS: Product performance metrics aggregated by item and category
CREATE OR REPLACE DYNAMIC TABLE tasty_bytes_db.analytics.product_performance_metrics
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = tasty_bytes_wh
  AS
SELECT
  -- Product dimensions
  menu_item_id,
  menu_item_name,
  item_category,
  item_subcategory,
  truck_brand_name,
  menu_type,
  -- Sales volume metrics
  COUNT(DISTINCT order_id) AS order_count,
  SUM(quantity) AS total_units_sold,
  -- Revenue and profit metrics
  SUM(line_total) AS total_revenue,
  SUM(line_profit) AS total_profit,
  ROUND(AVG(unit_price), 2) AS avg_unit_price,
  ROUND(AVG(profit_margin_pct), 2) AS avg_profit_margin_pct,
  -- Cost metrics
  AVG(cost_of_goods_usd) AS avg_cogs,
  AVG(sale_price_usd) AS standard_sale_price,
  -- Performance indicators
  SUM(line_total) / NULLIF(SUM(quantity), 0) AS revenue_per_unit,
  SUM(line_profit) / NULLIF(SUM(quantity), 0) AS profit_per_unit
FROM tasty_bytes_db.analytics.order_fact
GROUP BY
  menu_item_id,
  menu_item_name,
  item_category,
  item_subcategory,
  truck_brand_name,
  menu_type;

/*
Sample queries to validate data in dynamic tables
 */

-- Tier 1: Check orders_enriched - enriched order headers
SELECT order_id, order_date, day_name, order_hour, order_amount, order_total, has_discount
FROM tasty_bytes_db.analytics.orders_enriched
ORDER BY order_timestamp DESC
LIMIT 10;

-- Tier 1: Check order_items_enriched - enriched line items with product details
SELECT menu_item_name, item_category, quantity, unit_price,
       line_total, line_profit, profit_margin_pct
FROM tasty_bytes_db.analytics.order_items_enriched
ORDER BY line_profit DESC
LIMIT 10;

-- Tier 2: Check order_fact - combined order header and line items
SELECT order_id, order_date, menu_item_name, item_category,
       quantity, order_total, line_profit, profit_margin_pct
FROM tasty_bytes_db.analytics.order_fact
ORDER BY order_timestamp DESC
LIMIT 10;

-- Tier 3: Check daily_business_metrics - shows pre-aggregated daily KPIs
SELECT order_date, day_name, total_orders, unique_customers,
       total_revenue, avg_order_value, total_profit, avg_profit_margin_pct
FROM tasty_bytes_db.analytics.daily_business_metrics
ORDER BY order_date DESC
LIMIT 10;

-- Tier 3: Check product_performance_metrics - shows product-level sales and profit analysis
SELECT menu_item_name, item_category, order_count, total_units_sold,
       total_revenue, total_profit, avg_profit_margin_pct
FROM tasty_bytes_db.analytics.product_performance_metrics
ORDER BY total_revenue DESC
LIMIT 10;