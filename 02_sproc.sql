/*
================================================================================
CORTEX CODE PROMPT
================================================================================
Copy the prompt below into Cortex Code (Cmd+L in your Workspace).
Review the generated SQL, then execute.
================================================================================

Create a stored procedure tasty_bytes_db.raw.generate_demo_orders(num_rows INTEGER) 
that generates synthetic orders for testing incremental refresh. It should:
- Capture row counts before insertion (order_header and order_detail)
- Sample num_rows random existing orders from order_header into a temp table
- Generate new unique order IDs (avoid conflicts with existing)
- Update timestamps to the current date while preserving time-of-day patterns
- Randomize prices ±20% to simulate realistic fluctuations
- Insert the new orders into order_header
- For each new order, copy corresponding order_detail records from the 
  original order, with new order_detail_ids and randomized prices
- Capture row counts after insertion
- Clean up the temp table
- Return a summary string showing orders inserted, line items inserted, 
  and total order count

Use SQL language, RETURNS STRING.

================================================================================
EXPECTED OUTPUT
The SQL below is what Cortex Code should generate. Your output may differ
slightly — verify referential integrity is maintained between order_header 
and order_detail.
================================================================================
*/

USE ROLE lab_role;
USE DATABASE tasty_bytes_db;
USE WAREHOUSE tasty_bytes_wh;

CREATE OR REPLACE PROCEDURE tasty_bytes_db.raw.generate_demo_orders(num_rows INTEGER)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  orders_before INTEGER;
  orders_after INTEGER;
  orders_inserted INTEGER;
  details_before INTEGER;
  details_after INTEGER;
  details_inserted INTEGER;
BEGIN
  SELECT COUNT(*) INTO :orders_before FROM tasty_bytes_db.raw.order_header;
  SELECT COUNT(*) INTO :details_before FROM tasty_bytes_db.raw.order_detail;

  CREATE OR REPLACE TEMPORARY TABLE new_orders AS
  SELECT
    (1000000 + UNIFORM(1, 999999, RANDOM()))::NUMBER(38,0) AS new_order_id,
    oh.order_id AS original_order_id,
    oh.truck_id,
    oh.location_id,
    oh.customer_id,
    oh.discount_id,
    oh.shift_id,
    oh.shift_start_time,
    oh.shift_end_time,
    oh.order_channel,
    DATEADD('day', DATEDIFF('day', oh.order_ts, CURRENT_DATE()), oh.order_ts) AS order_ts,
    oh.served_ts,
    oh.order_currency,
    oh.order_amount * (0.8 + UNIFORM(0, 0.4, RANDOM())) AS order_amount,
    oh.order_tax_amount,
    oh.order_discount_amount,
    oh.order_total * (0.8 + UNIFORM(0, 0.4, RANDOM())) AS order_total
  FROM tasty_bytes_db.raw.order_header oh
  WHERE oh.order_id IS NOT NULL
  ORDER BY RANDOM()
  LIMIT :num_rows;

  INSERT INTO tasty_bytes_db.raw.order_header (
    order_id, truck_id, location_id, customer_id, discount_id, shift_id,
    shift_start_time, shift_end_time, order_channel, order_ts, served_ts,
    order_currency, order_amount, order_tax_amount, order_discount_amount,
    order_total
  )
  SELECT
    new_order_id, truck_id, location_id, customer_id, discount_id, shift_id,
    shift_start_time, shift_end_time, order_channel, order_ts, served_ts,
    order_currency, order_amount, order_tax_amount, order_discount_amount,
    order_total
  FROM new_orders;

  INSERT INTO tasty_bytes_db.raw.order_detail (
    order_detail_id, order_id, menu_item_id, discount_id, line_number,
    quantity, unit_price, price, order_item_discount_amount
  )
  SELECT
    (2000000 + UNIFORM(1, 9999999, RANDOM()))::NUMBER(38,0) AS order_detail_id,
    no.new_order_id AS order_id,
    od.menu_item_id,
    od.discount_id,
    od.line_number,
    od.quantity,
    od.unit_price * (0.8 + UNIFORM(0, 0.4, RANDOM())) AS unit_price,
    od.price * (0.8 + UNIFORM(0, 0.4, RANDOM())) AS price,
    od.order_item_discount_amount
  FROM new_orders no
  INNER JOIN tasty_bytes_db.raw.order_detail od
    ON no.original_order_id = od.order_id;

  SELECT COUNT(*) INTO :orders_after FROM tasty_bytes_db.raw.order_header;
  SELECT COUNT(*) INTO :details_after FROM tasty_bytes_db.raw.order_detail;

  orders_inserted := :orders_after - :orders_before;
  details_inserted := :details_after - :details_before;

  DROP TABLE IF EXISTS new_orders;

  RETURN 'Successfully generated ' || orders_inserted::STRING || ' new orders with ' ||
         details_inserted::STRING || ' line items. Total orders: ' || orders_after::STRING;
END;
$$;
