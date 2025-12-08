# Declarative Pipelines with Snowflake Dynamic Tables

This lab demonstrates building a declarative data pipeline using Snowflake Dynamic Tables with incremental refresh capabilities, monitoring, and querying via Snowflake Intelligence.

## Prerequisites

- Snowflake account with ACCOUNTADMIN access
- Ability to create databases, warehouses, and roles
- Basic SQL knowledge

## How to run this repo

The easiest way to build the data pipeline in this repo is to connect this repo to Snowflake via a Snowflake Workspace. Follow the instructions here: https://docs.snowflake.com/en/user-guide/ui-snowsight/workspaces-git#create-a-git-workspace

**Important:** When prompted to select an authentication method, select **Public repository**.

## Files

**00_load_tasty_bytes.sql**
- Creates lab role, database (tasty_bytes_db), schemas (raw, analytics), and warehouse (tasty_bytes_wh)
- Defines raw table structures for order_header, order_detail, and menu
- Sets up external stage and file format for CSV data ingestion
- Loads approximately 1B+ records from public S3 bucket


**01_dynamic_tables.sql**
- Creates 3-tier declarative pipeline using Dynamic Tables
- Tier 1: Enriches raw data with temporal dimensions, financial calculations, and discount flags
- Tier 2: Joins enriched orders and line items into comprehensive fact table
- Tier 3: Pre-aggregates daily business metrics and product performance metrics
- Uses TARGET_LAG for time-based refresh (12 hours) and DOWNSTREAM for dependency-based refresh
- Demonstrates automatic dependency graph management

**02_sproc.sql**
- Creates stored procedure generate_demo_orders(num_rows) to simulate new order arrivals
- Generates synthetic order headers and corresponding order details with referential integrity
- Used to demonstrate incremental refresh capabilities

**03_incremental_refresh.sql**
- Demonstrates incremental vs full refresh behavior
- Inserts 500 new orders using stored procedure
- Manually triggers refresh on all dynamic tables across three tiers
- Queries INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY to show INCREMENTAL vs FULL refresh actions
- Validates new data propagation through entire pipeline

**04_monitoring.sql**
- Queries dynamic table metadata and refresh history
- Shows refresh type (INCREMENTAL vs FULL), duration, and current state

**05_intelligence.sql**
- Sets up Snowflake Intelligence infrastructure (database, schema, grants)
- Includes 10 sample business questions for testing agent capabilities

**06_cleanup.sql**
- Drops all lab resources: tasty_bytes_db, snowflake_intelligence database, and tasty_bytes_wh warehouse
- Optional role cleanup (requires ACCOUNTADMIN)
- Resets environment to clean state

### Python Scripts

**streamlit.py**
- Streamlit dashboard visualizing Tasty Bytes analytics
- Displays top 10 products by revenue with Altair bar chart colored by profit margin
- Shows current day key metrics: orders, revenue, profit, margin, customers, items sold
- Uses Snowpark for data access from dynamic tables

### Configuration Files

**semantic_model.yaml**
- Semantic model defining all 5 dynamic tables with comprehensive metadata
- Includes dimensions, time_dimensions, facts with synonyms and descriptions
- Contains 10 verified queries for common business questions
