/*
05_intelligence.sql

Sets up the necessary infrastructure for Snowflake Intelligence.
After running this script, create your agent via the Snowsight UI.
*/

USE ROLE ACCOUNTADMIN;

-- Grant Intelligence privileges to lab_role
GRANT CREATE DATABASE ON ACCOUNT TO ROLE lab_role;

USE ROLE lab_role;
USE WAREHOUSE tasty_bytes_wh;

-- Create Intelligence objects
CREATE DATABASE IF NOT EXISTS snowflake_intelligence;
CREATE SCHEMA IF NOT EXISTS snowflake_intelligence.agents;

GRANT USAGE ON DATABASE snowflake_intelligence TO ROLE lab_role;
GRANT USAGE ON SCHEMA snowflake_intelligence.agents TO ROLE lab_role;
GRANT CREATE AGENT ON SCHEMA snowflake_intelligence.agents TO ROLE lab_role;

/*
Sample questions to ask your Intelligence agent:

1. "What was the total revenue for the last 30 days?"
2. "Which products have the highest profit margins?"
3. "Show me daily revenue trends as a line chart"
4. "How many unique customers did we have yesterday?"
5. "What percentage of orders include discounts?"
6. "Compare revenue by product category"
7. "Show me the top 5 products by profit"
8. "What are the busiest hours for orders?"
9. "How does revenue vary by day of week?"
10. "Which truck brands generate the most profit?"
*/
