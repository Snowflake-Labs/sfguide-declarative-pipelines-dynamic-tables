/*
================================================================================
CORTEX CODE PROMPT
================================================================================
Copy the prompt below into Cortex Code.
================================================================================

Create a stored procedure tasty_bytes_db.raw.generate_demo_orders(num_rows INTEGER)
using SQL language that generates synthetic orders. It should:
- Capture row counts in order_header and order_detail before insertion
- Sample num_rows random existing orders from order_header into a temporary table
- Assign new unique order IDs to avoid conflicts with existing data
- Set timestamps to the current date while preserving the original time-of-day
- Apply random price variation of plus or minus 20% using UNIFORM
- Insert the new orders into order_header
- For each new order, copy the corresponding order_detail records from the
  original order, generating new order_detail_ids and applying the same
  price randomization
- Capture row counts after insertion
- Clean up the temporary table
- Return a summary string showing how many orders and line items were inserted

================================================================================
Reference SQL is available in the solutions branch.
================================================================================
*/
