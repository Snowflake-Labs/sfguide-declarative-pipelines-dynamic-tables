/*
================================================================================
CORTEX CODE PROMPT
================================================================================
Copy the prompt below into Cortex Code (Cmd+L in your Workspace).
Review the generated SQL, then execute.
================================================================================

Using ACCOUNTADMIN, set up a lab environment:
- Create a role called lab_role and grant it to SYSADMIN
- Grant lab_role CREATE WAREHOUSE and CREATE DATABASE on account
- Switch to lab_role for all remaining operations
- Create database tasty_bytes_db with schemas raw and analytics
- Create a 2XL standard warehouse tasty_bytes_wh with 60s auto-suspend, 
  auto-resume, initially suspended
- Create a CSV file format in tasty_bytes_db.public
- Create an external stage tasty_bytes_db.raw.tasty_bytes_stage pointing to 
  s3://sfquickstarts/tasty-bytes-builder-education/
- Create tables order_header, order_detail, and menu in tasty_bytes_db.raw 
  (use appropriate column types for order IDs, timestamps, amounts, etc.)
- Load all 3 tables using COPY INTO from the stage subdirectories:
  raw_pos/order_header/, raw_pos/order_detail/, raw_pos/menu/

================================================================================
EXPECTED OUTPUT
The SQL below is what Cortex Code should generate. Your output may differ
slightly — verify the key elements match (role setup, table structures, stage URL).
================================================================================
*/

USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS lab_role;
GRANT ROLE lab_role TO ROLE SYSADMIN;

GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE lab_role;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE lab_role;

USE ROLE lab_role;

CREATE DATABASE IF NOT EXISTS tasty_bytes_db;
CREATE SCHEMA IF NOT EXISTS tasty_bytes_db.raw;
CREATE SCHEMA IF NOT EXISTS tasty_bytes_db.analytics;

USE DATABASE tasty_bytes_db;

CREATE OR REPLACE WAREHOUSE tasty_bytes_wh
   WAREHOUSE_SIZE = '2x-large'
   WAREHOUSE_TYPE = 'standard'
   AUTO_SUSPEND = 60
   AUTO_RESUME = TRUE
   INITIALLY_SUSPENDED = TRUE;

CREATE OR REPLACE FILE FORMAT tasty_bytes_db.public.csv_ff
type = 'csv';

CREATE OR REPLACE STAGE tasty_bytes_db.raw.tasty_bytes_stage
  URL = 's3://sfquickstarts/tasty-bytes-builder-education/'
  FILE_FORMAT = tasty_bytes_db.public.csv_ff;

CREATE OR REPLACE TABLE tasty_bytes_db.raw.order_header
(
   order_id NUMBER(38,0),
   truck_id NUMBER(38,0),
   location_id FLOAT,
   customer_id NUMBER(38,0),
   discount_id VARCHAR(16777216),
   shift_id NUMBER(38,0),
   shift_start_time TIME(9),
   shift_end_time TIME(9),
   order_channel VARCHAR(16777216),
   order_ts TIMESTAMP_NTZ(9),
   served_ts VARCHAR(16777216),
   order_currency VARCHAR(3),
   order_amount NUMBER(38,4),
   order_tax_amount VARCHAR(16777216),
   order_discount_amount VARCHAR(16777216),
   order_total NUMBER(38,4)
);

CREATE OR REPLACE TABLE tasty_bytes_db.raw.order_detail
(
   order_detail_id NUMBER(38,0),
   order_id NUMBER(38,0),
   menu_item_id NUMBER(38,0),
   discount_id VARCHAR(16777216),
   line_number NUMBER(38,0),
   quantity NUMBER(5,0),
   unit_price NUMBER(38,4),
   price NUMBER(38,4),
   order_item_discount_amount VARCHAR(16777216)
);

CREATE OR REPLACE TABLE tasty_bytes_db.raw.menu
(
   menu_id NUMBER(19,0),
   menu_type_id NUMBER(38,0),
   menu_type VARCHAR(16777216),
   truck_brand_name VARCHAR(16777216),
   menu_item_id NUMBER(38,0),
   menu_item_name VARCHAR(16777216),
   item_category VARCHAR(16777216),
   item_subcategory VARCHAR(16777216),
   cost_of_goods_usd NUMBER(38,4),
   sale_price_usd NUMBER(38,4),
   menu_item_health_metrics_obj VARIANT
);

USE WAREHOUSE tasty_bytes_wh;

COPY INTO tasty_bytes_db.raw.order_header
FROM @tasty_bytes_db.raw.tasty_bytes_stage/raw_pos/order_header/;

COPY INTO tasty_bytes_db.raw.order_detail
FROM @tasty_bytes_db.raw.tasty_bytes_stage/raw_pos/order_detail/;

COPY INTO tasty_bytes_db.raw.menu
FROM @tasty_bytes_db.raw.tasty_bytes_stage/raw_pos/menu/;

SELECT 'Data loaded successfully' AS status,
       (SELECT COUNT(*) FROM tasty_bytes_db.raw.order_header) AS order_header_rows,
       (SELECT COUNT(*) FROM tasty_bytes_db.raw.order_detail) AS order_detail_rows,
       (SELECT COUNT(*) FROM tasty_bytes_db.raw.menu) AS menu_rows;
