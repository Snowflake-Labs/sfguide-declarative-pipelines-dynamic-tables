/*
00_load_tasty_bytes.sql

Sets up the lab environment by creating the necessary database, schemas,
warehouse, raw tables, and loading Tasty Bytes data from S3.
*/

USE ROLE ACCOUNTADMIN;

-- Create lab role
CREATE ROLE IF NOT EXISTS lab_role;
GRANT ROLE lab_role TO ROLE SYSADMIN;

-- Grant warehouse privileges
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE lab_role;

-- Grant database and schema privileges
GRANT CREATE DATABASE ON ACCOUNT TO ROLE lab_role;

-- Grant task execution privileges (required for scheduled tasks)
-- GRANT EXECUTE TASK ON ACCOUNT TO ROLE lab_role;

-- Switch to lab_role for all subsequent operations
USE ROLE lab_role;

-- Create database and schemas
CREATE DATABASE IF NOT EXISTS tasty_bytes_db;
CREATE SCHEMA IF NOT EXISTS tasty_bytes_db.raw;
CREATE SCHEMA IF NOT EXISTS tasty_bytes_db.analytics;

USE DATABASE tasty_bytes_db;

CREATE OR REPLACE WAREHOUSE tasty_bytes_wh
   WAREHOUSE_SIZE = 'xlarge'
   WAREHOUSE_TYPE = 'standard'
   AUTO_SUSPEND = 60
   AUTO_RESUME = TRUE
   INITIALLY_SUSPENDED = TRUE;

-- File format for CSV files
CREATE OR REPLACE FILE FORMAT tasty_bytes_db.public.csv_ff
type = 'csv';

-- External stage pointing to public S3 bucket
CREATE OR REPLACE STAGE tasty_bytes_db.raw.tasty_bytes_stage
  URL = 's3://sfquickstarts/tasty-bytes-builder-education/'
  FILE_FORMAT = tasty_bytes_db.public.csv_ff;

-- Stage for Cortex Analyst semantic model files
CREATE OR REPLACE STAGE tasty_bytes_db.analytics.semantic_models
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Stage for uploading Cortex Analyst semantic model YAML files';

-- Table definitions for raw data
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

-- Load data into tables from CSV files in S3
COPY INTO tasty_bytes_db.raw.order_header
FROM @tasty_bytes_db.raw.tasty_bytes_stage/raw_pos/order_header/;

COPY INTO tasty_bytes_db.raw.order_detail
FROM @tasty_bytes_db.raw.tasty_bytes_stage/raw_pos/order_detail/;

COPY INTO tasty_bytes_db.raw.menu
FROM @tasty_bytes_db.raw.tasty_bytes_stage/raw_pos/menu/;

-- Optional: Verify data loaded
SELECT * FROM tasty_bytes_db.raw.order_header LIMIT 10;
SELECT * FROM tasty_bytes_db.raw.order_detail LIMIT 10;
SELECT * FROM tasty_bytes_db.raw.menu LIMIT 10;

-- Example task to load data from S3 every 12 hours
/*
CREATE OR REPLACE TASK tasty_bytes_db.raw.load_tasty_bytes_task
  WAREHOUSE = tasty_bytes_wh
  SCHEDULE = '720 MINUTE'
  COMMENT = 'Loads order data from S3 every 12 hours'
AS
BEGIN
  COPY INTO tasty_bytes_db.raw.order_header
  FROM @tasty_bytes_db.raw.tasty_bytes_stage/raw_pos/order_header/;

  COPY INTO tasty_bytes_db.raw.order_detail
  FROM @tasty_bytes_db.raw.tasty_bytes_stage/raw_pos/order_detail/;

  COPY INTO tasty_bytes_db.raw.menu
  FROM @tasty_bytes_db.raw.tasty_bytes_stage/raw_pos/menu/;
END;
*/
