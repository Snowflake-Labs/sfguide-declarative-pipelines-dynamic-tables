/*
================================================================================
CORTEX CODE PROMPT
================================================================================
Copy the prompt below into Cortex Code.
================================================================================

Build a 3-tier dynamic table pipeline in tasty_bytes_db.analytics using
warehouse tasty_bytes_wh:

Use INITIALIZE = 'ON_SCHEDULE' for all dynamic tables.

Tier 1 - Enrichment (TARGET_LAG = DOWNSTREAM):
- orders_enriched: From tasty_bytes_db.raw.order_header. Include order_id,
  truck_id, customer_id, order_channel. Add temporal dimensions: order_date,
  day_name (DAYNAME), order_hour (HOUR), and order_ts as order_timestamp.
  Include order_amount and order_total. Cast order_discount_amount to NUMBER(10,2)
  using TRY_TO_NUMBER. Add a has_discount boolean (true when discount_id is not
  null and not empty string). Filter out null order_id and null order_ts.

- order_items_enriched: Join tasty_bytes_db.raw.order_detail with
  tasty_bytes_db.raw.menu on menu_item_id. Include order_detail_id, order_id,
  line_number, menu_item_id, menu_item_name, item_category, item_subcategory,
  truck_brand_name, menu_type, quantity, unit_price, price as line_total,
  cost_of_goods_usd, sale_price_usd. Calculate unit_profit (unit_price minus
  cost_of_goods_usd), line_profit (unit_profit times quantity), and
  profit_margin_pct (rounded to 2 decimals, handle zero unit_price). Cast
  order_item_discount_amount to NUMBER(10,2) as line_discount_amount. Add
  has_discount flag. Filter out null order_id and null menu_item_id.

Tier 2 - Fact Table (TARGET_LAG = DOWNSTREAM):
- order_fact: Inner join orders_enriched (alias o) with order_items_enriched
  (alias oi) on order_id. Include all columns from both. Rename
  o.order_discount_amount as order_level_discount and o.has_discount as
  order_has_discount, oi.has_discount as line_has_discount.

Tier 3 - Aggregated Metrics (TARGET_LAG = 1 hour):
- daily_business_metrics: Aggregate order_fact by order_date and day_name.
  Include count distinct order_id (total_orders), truck_id (active_trucks),
  customer_id (unique_customers), sum quantity (total_items_sold), sum
  order_total (total_revenue), avg order_total (avg_order_value), sum
  line_total (total_line_item_revenue), sum line_profit (total_profit),
  avg profit_margin_pct (avg_profit_margin_pct), count orders with discount,
  sum discount amounts.

- product_performance_metrics: Aggregate order_fact by menu_item_id,
  menu_item_name, item_category, item_subcategory, truck_brand_name, menu_type.
  Include count distinct order_id (order_count), sum quantity (total_units_sold),
  sum line_total (total_revenue), sum line_profit (total_profit), avg unit_price,
  avg profit_margin_pct, avg cost_of_goods_usd (avg_cogs), avg sale_price_usd
  (standard_sale_price), revenue_per_unit, profit_per_unit.

================================================================================

After CoCo creates the 5 dynamic tables, trigger the initial load with this prompt:

Refresh daily_business_metrics and product_performance_metrics in
tasty_bytes_db.analytics. Snowflake will handle the upstream dependencies.

Note: You only need to trigger the top-tier tables — Snowflake automatically
cascades through Tier 1 and Tier 2 first due to the DOWNSTREAM dependency model.

================================================================================
*/
