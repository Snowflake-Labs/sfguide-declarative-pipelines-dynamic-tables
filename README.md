# Build Autonomous SQL Pipelines with Cortex Code & Dynamic Tables

This lab demonstrates building a declarative data pipeline using Snowflake Dynamic Tables — driven entirely by natural language prompts to **Cortex Code** inside Snowsight Workspaces.

Instead of manually writing SQL, you describe your pipeline intent to Cortex Code and it generates, validates, and executes the SQL for you.

## Prerequisites

- Snowflake account with ACCOUNTADMIN access ([free trial](https://signup.snowflake.com/developers))
- Cortex Code enabled ([setup guide](https://docs.snowflake.com/en/user-guide/cortex-code))
- Basic familiarity with data engineering concepts

## How It Works

Each SQL file in this repo follows a **prompt-first pattern**:

```sql
/*
================================================================================
CORTEX CODE PROMPT
================================================================================
<natural language prompt to give to Cortex Code>
================================================================================
EXPECTED OUTPUT
<description of what CoCo should produce>
================================================================================
*/

-- The expected SQL follows below...
```

**Workflow:**
1. Create a Snowsight Workspace from this repository
2. Open the Cortex Code panel (Cmd+L)
3. Open a SQL file — copy the prompt at the top into CoCo
4. Review CoCo's generated SQL against the expected output below
5. Execute when satisfied

The SQL files remain fully runnable on their own for anyone who prefers the traditional approach.

## Files

| File | Purpose | CoCo Approach |
|:-----|:--------|:--------------|
| `00_setup_environment.sql` | Role, DB, warehouse, tables, data load | Direct execution |
| `01_dynamic_tables.sql` | 3-tier pipeline (5 dynamic tables) | Generate-then-confirm |
| `02_sproc.sql` | Stored procedure for synthetic test data | Generate-then-confirm |
| `03_incremental_refresh.sql` | Test incremental refresh capabilities | Sequential prompts |
| `04_monitoring.sql` | Pipeline monitoring queries | Direct execution |
| `05_semantic_view_agent.sql` | Semantic view + Cortex Agent creation | Generate-then-confirm |
| `06_cleanup.sql` | Drop all lab resources | Direct execution |

## Quick Start

1. Navigate to **Projects > Workspaces** in Snowsight
2. Click **Create > From Git repository**
3. Enter: `https://github.com/Snowflake-Labs/sfguide-declarative-pipelines-dynamic-tables`
4. Select **Public repository**
5. Open Cortex Code (Cmd+L) and start with `00_setup_environment.sql`

## What You'll Build

- A three-tier declarative data pipeline processing ~1B order records
- Stored procedures for generating test data
- Incremental refresh validation
- Monitoring queries for pipeline observability
- A semantic view for natural language querying
- A Cortex Agent for conversational data exploration

## Related Resources

- [Quickstart Guide](https://www.snowflake.com/en/developers/guides/snowflake-dynamic-tables-data-pipeline/)
- [Dynamic Tables Documentation](https://docs.snowflake.com/en/user-guide/dynamic-tables-intro)
- [Cortex Code Documentation](https://docs.snowflake.com/en/user-guide/cortex-code)
- [Cortex Analyst Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)

## License

Apache 2.0
