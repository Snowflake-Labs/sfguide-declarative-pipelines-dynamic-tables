/*
================================================================================
CORTEX CODE PROMPT
================================================================================
Copy the prompt below into Cortex Code (Cmd+L in your Workspace).
Review the generated SQL, then execute.
================================================================================

Create a semantic view called tasty_bytes_semantic_model in 
TASTY_BYTES_DB.ANALYTICS over all 5 dynamic tables in the analytics schema:
- daily_business_metrics
- product_performance_metrics  
- order_fact
- orders_enriched
- order_items_enriched

Then create a Cortex Agent called tasty_bytes_agent in TASTY_BYTES_DB.ANALYTICS 
that uses the tasty_bytes_semantic_model semantic view as its Cortex Analyst 
tool. Set the display name to "Tasty Bytes Analytics Agent".

================================================================================
EXPECTED OUTPUT
Cortex Code will use its semantic-view and cortex-agent skills to create 
these objects. The exact SQL will vary based on the CoCo skills used.
Below are sample questions to test your agent with.
================================================================================
*/

-- After CoCo creates the semantic view and agent, test with these questions:

-- "What are the top 10 products by revenue?"
-- "Show me daily revenue trends for the last 30 days"
-- "Which truck brands are most profitable?"
-- "How many unique customers do we have?"
-- "What's the average order value by day of week?"
-- "What percentage of orders include discounts?"
-- "Compare revenue by product category"
-- "What are the busiest hours for orders?"
