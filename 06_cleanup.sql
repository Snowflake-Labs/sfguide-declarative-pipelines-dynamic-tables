/*
================================================================================
CORTEX CODE PROMPT
================================================================================
Copy the prompt below into Cortex Code (Cmd+L in your Workspace).
CoCo will execute directly.
================================================================================

Clean up: drop database tasty_bytes_db and warehouse tasty_bytes_wh using 
lab_role. Then using ACCOUNTADMIN, drop the lab_role.

================================================================================
EXPECTED OUTPUT
================================================================================
*/

USE ROLE lab_role;

DROP DATABASE IF EXISTS tasty_bytes_db;
DROP WAREHOUSE IF EXISTS tasty_bytes_wh;

USE ROLE ACCOUNTADMIN;
DROP ROLE IF EXISTS lab_role;
