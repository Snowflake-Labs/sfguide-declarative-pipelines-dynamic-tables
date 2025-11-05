/*
05_cleanup.sql

Cleanup script to remove all objects created for the lab. Run this to reset
your environment to a clean state.

WARNING: This will delete all data and objects. Use with caution!
*/

USE ROLE lab_role;

-- Drop the entire database (cascades to all schemas, tables, dynamic tables, views, stages, etc.)
DROP DATABASE IF EXISTS tasty_bytes_db;

-- Drop the warehouse
DROP WAREHOUSE IF EXISTS tasty_bytes_wh;

-- Optionally drop the role (requires ACCOUNTADMIN)
-- USE ROLE ACCOUNTADMIN;
-- DROP ROLE IF EXISTS lab_role;
