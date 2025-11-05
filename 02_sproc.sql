/*
02_sproc.sql

Stored procedure to simulate new orders landing into raw tables,
to demonstrate incremental refresh in dynamic tables.
*/

USE ROLE lab_role;
USE DATABASE tasty_bytes_db;
USE WAREHOUSE tasty_bytes_wh;

-- Stored procedure to generate N synthetic orders with corresponding order details
-- Example usage: CALL tasty_bytes_db.raw.generate_demo_orders(500);
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
  -- Capture counts before insert
  SELECT COUNT(*) INTO :orders_before FROM tasty_bytes_db.raw.order_header;
  SELECT COUNT(*) INTO :details_before FROM tasty_bytes_db.raw.order_detail;
  -- Create temporary table with new order IDs to maintain referential integrity
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
  -- Insert synthetic order headers
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
  -- Insert corresponding order details (line items)
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

  -- Capture counts after insert
  SELECT COUNT(*) INTO :orders_after FROM tasty_bytes_db.raw.order_header;
  SELECT COUNT(*) INTO :details_after FROM tasty_bytes_db.raw.order_detail;

  orders_inserted := :orders_after - :orders_before;
  details_inserted := :details_after - :details_before;

  -- Clean up temporary table
  DROP TABLE IF EXISTS new_orders;

  RETURN 'Successfully generated ' || orders_inserted::STRING || ' new orders with ' ||
         details_inserted::STRING || ' line items. Total orders: ' || orders_after::STRING;
END;
$$;