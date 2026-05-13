/*
================================================================================
CORTEX CODE PROMPT
================================================================================
Copy the prompt below into Cortex Code (Cmd+L in your Workspace).
Review the generated SQL, then execute.
================================================================================

Build a 3-tier dynamic table pipeline in tasty_bytes_db.analytics using 
warehouse tasty_bytes_wh:

Tier 1 - Enrichment (TARGET_LAG = DOWNSTREAM):
- orders_enriched: From raw.order_header. Include order_id, truck_id, 
  customer_id, order_channel. Add temporal dimensions: order_ts as 
  order_timestamp, DATE(order_ts) as order_date, DAYNAME as day_name, 
  HOUR as order_hour. Include order_amount and order_total. Cast 
  order_discount_amount to NUMBER(10,2) using TRY_TO_NUMBER. Add a 
  has_discount boolean (true when discount_id is not null and not empty). 
  Filter out null order_id and null order_ts.

- order_items_enriched: Join raw.order_detail with raw.menu on menu_item_id.
  Include order_detail_id, order_id, line_number, menu_item_id, 
  menu_item_name, item_category, item_subcategory, truck_brand_name, 
  menu_type, quantity, unit_price, price as line_total, cost_of_goods_usd, 
  sale_price_usd. Calculate unit_profit (unit_price - cost_of_goods_usd), 
  line_profit (unit_profit * quantity), profit_margin_pct (percentage with 
  2 decimal places, handle zero unit_price). Cast order_item_discount_amount 
  to NUMBER(10,2) as line_discount_amount. Add has_discount flag. Filter 
  out null order_id and null menu_item_id.

Tier 2 - Fact Table (TARGET_LAG = DOWNSTREAM):
- order_fact: Inner join orders_enriched (alias o) with order_items_enriched 
  (alias oi) on order_id. Include all fields from both tables. Rename 
  o.order_discount_amount as order_level_discount, o.has_discount as 
  order_has_discount, oi.has_discount as line_has_discount.

Tier 3 - Aggregated Metrics (TARGET_LAG = 1 hour):
- daily_business_metrics: Aggregate order_fact by order_date, day_name. 
  Include count distinct order_id, truck_id, customer_id. Sum quantity, 
  order_total, line_total, line_profit. Avg order_total, profit_margin_pct. 
  Count orders with discount. Sum discount amounts.

- product_performance_metrics: Aggregate order_fact by menu_item_id, 
  menu_item_name, item_category, item_subcategory, truck_brand_name, 
  menu_type. Include count distinct order_id, sum quantity, sum line_total, 
  sum line_profit, avg unit_price, avg profit_margin_pct, avg 
  cost_of_goods_usd, avg sale_price_usd, revenue_per_unit, profit_per_unit.

================================================================================
EXPECTED OUTPUT
The SQL below is what Cortex Code should generate. Your output may differ
slightly — verify the key elements match (TARGET_LAG values, join conditions,
column calculations, dependency flow).
================================================================================
*/

USE ROLE lab_role;
USE DATABASE tasty_bytes_db;
USE WAREHOUSE tasty_bytes_wh;

-- Tier 1: ORDERS_ENRICHED
CREATE OR REPLACE DYNAMIC TABLE tasty_bytes_db.analytics.orders_enriched
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = tasty_bytes_wh
  AS
SELECT
  order_id,
  truck_id,
  customer_id,
  order_channel,
  order_ts AS order_timestamp,
  DATE(order_ts) AS order_date,
  DAYNAME(order_ts) AS day_name,
  HOUR(order_ts) AS order_hour,
  order_amount,
  order_total,
  TRY_TO_NUMBER(order_discount_amount, 10, 2) AS order_discount_amount,
  CASE
    WHEN discount_id IS NOT NULL AND discount_id != '' THEN TRUE
    ELSE FALSE
  END AS has_discount
FROM tasty_bytes_db.raw.order_header
WHERE order_id IS NOT NULL
  AND order_ts IS NOT NULL;


-- Tier 1: ORDER_ITEMS_ENRICHED
CREATE OR REPLACE DYNAMIC TABLE tasty_bytes_db.analytics.order_items_enriched
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = tasty_bytes_wh
  AS
SELECT
  od.order_detail_id,
  od.order_id,
  od.line_number,
  od.menu_item_id,
  m.menu_item_name,
  m.item_category,
  m.item_subcategory,
  m.truck_brand_name,
  m.menu_type,
  od.quantity,
  od.unit_price,
  od.price AS line_total,
  m.cost_of_goods_usd,
  m.sale_price_usd,
  (od.unit_price - m.cost_of_goods_usd) AS unit_profit,
  (od.unit_price - m.cost_of_goods_usd) * od.quantity AS line_profit,
  CASE
    WHEN od.unit_price > 0 THEN
      ROUND(((od.unit_price - m.cost_of_goods_usd) / od.unit_price) * 100, 2)
    ELSE 0
  END AS profit_margin_pct,
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


-- Tier 2: ORDER_FACT
CREATE OR REPLACE DYNAMIC TABLE tasty_bytes_db.analytics.order_fact
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE = tasty_bytes_wh
  AS
SELECT
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


-- Tier 3: DAILY_BUSINESS_METRICS
CREATE OR REPLACE DYNAMIC TABLE tasty_bytes_db.analytics.daily_business_metrics
  TARGET_LAG = '1 hour'
  WAREHOUSE = tasty_bytes_wh
  AS
SELECT
  order_date,
  day_name,
  COUNT(DISTINCT order_id) AS total_orders,
  COUNT(DISTINCT truck_id) AS active_trucks,
  COUNT(DISTINCT customer_id) AS unique_customers,
  SUM(quantity) AS total_items_sold,
  SUM(order_total) AS total_revenue,
  ROUND(AVG(order_total), 2) AS avg_order_value,
  SUM(line_total) AS total_line_item_revenue,
  SUM(line_profit) AS total_profit,
  ROUND(AVG(profit_margin_pct), 2) AS avg_profit_margin_pct,
  SUM(CASE WHEN order_has_discount THEN 1 ELSE 0 END) AS orders_with_discount,
  SUM(order_level_discount) AS total_order_discount_amount,
  SUM(line_discount_amount) AS total_line_discount_amount
FROM tasty_bytes_db.analytics.order_fact
GROUP BY order_date, day_name;


-- Tier 3: PRODUCT_PERFORMANCE_METRICS
CREATE OR REPLACE DYNAMIC TABLE tasty_bytes_db.analytics.product_performance_metrics
  TARGET_LAG = '1 hour'
  WAREHOUSE = tasty_bytes_wh
  AS
SELECT
  menu_item_id,
  menu_item_name,
  item_category,
  item_subcategory,
  truck_brand_name,
  menu_type,
  COUNT(DISTINCT order_id) AS order_count,
  SUM(quantity) AS total_units_sold,
  SUM(line_total) AS total_revenue,
  SUM(line_profit) AS total_profit,
  ROUND(AVG(unit_price), 2) AS avg_unit_price,
  ROUND(AVG(profit_margin_pct), 2) AS avg_profit_margin_pct,
  AVG(cost_of_goods_usd) AS avg_cogs,
  AVG(sale_price_usd) AS standard_sale_price,
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
