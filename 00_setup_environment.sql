/*
================================================================================
CORTEX CODE PROMPT
================================================================================
Copy the prompt below into Cortex Code.
================================================================================

Using ACCOUNTADMIN, create a role called lab_role and grant it CREATE WAREHOUSE
and CREATE DATABASE privileges on the account. Then switch to lab_role and:
- Create a database called tasty_bytes_db with two schemas: raw and analytics
- Create a 2XL standard warehouse called tasty_bytes_wh with 60s auto-suspend
  and auto-resume enabled
- Create these tables in tasty_bytes_db.raw:
  - order_header: columns for order_id, truck_id, location_id, customer_id,
    discount_id, shift_id, shift_start_time, shift_end_time, order_channel,
    order_ts (TIMESTAMP_NTZ), served_ts, order_currency, order_amount (NUMBER),
    order_tax_amount, order_discount_amount, order_total (NUMBER)
  - order_detail: columns for order_detail_id, order_id, menu_item_id,
    discount_id, line_number, quantity, unit_price, price, order_item_discount_amount
  - menu: columns for menu_id, menu_type_id, menu_type, truck_brand_name,
    menu_item_id, menu_item_name, item_category, item_subcategory,
    cost_of_goods_usd, sale_price_usd, menu_item_health_metrics_obj (VARIANT)
- Create a CSV file format in tasty_bytes_db.public
- Create an external stage tasty_bytes_db.raw.tasty_bytes_stage pointing to
  s3://sfquickstarts/tasty-bytes-builder-education/ using the CSV file format
- Load all three tables using COPY INTO from the stage subdirectories:
  raw_pos/order_header/, raw_pos/order_detail/, raw_pos/menu/

================================================================================
Reference SQL is available in the solutions branch.
================================================================================
*/
