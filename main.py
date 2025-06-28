#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["fastmcp", "snowflake-connector-python", "snowflake-core"]
# ///
import datetime

from mcp.server.fastmcp import FastMCP

from utils.sf import QueryResponse, Snowflake

sf = Snowflake()
mcp = FastMCP("snowflake")


@mcp.tool()
def get_databases() -> QueryResponse:
    """Get List of Snowflake Databases"""
    return sf.execute_query(
        """\
select
    database_name as name,
    database_owner as owner,
    comment,
    created,
    last_altered,
    retention_time as retention_time_in_days
from snowflake.information_schema.databases"""
    )


@mcp.tool()
def get_snowflake_schemas(database: str) -> QueryResponse:
    """Get List of Snowflake defined Schemas in a Snowflake Database"""
    return sf.execute_query(
        f"""\
select
    {database} as database,
    schema_name as name,
    schema_owner as owner,
    comment,
    created,
    last_altered,
    retention_time as retention_time_in_days
from {database}.information_schema.schemata
where created is null"""
    )


@mcp.tool()
def get_user_defined_schemas(database: str) -> QueryResponse:
    """Get List of Snowflake User Defined Schemas in a Snowflake Database"""
    return sf.execute_query(
        f"""\
select
    {database} as database,
    schema_name as name,
    schema_owner as owner,
    comment,
    created,
    last_altered,
    retention_time as retention_time_in_days
from {database}.information_schema.schemata
where created is not null"""
    )


@mcp.tool()
def get_tables(database: str, db_schema: str) -> QueryResponse:
    """Get List of Snowflake Tables in a Snowflake Database Schema"""
    return sf.execute_query(
        f"""\
select
    {database} as database,
    {db_schema} as schema,
    table_name as name,
    table_owner as owner,
    table_type,
    clustering_key,
    auto_clustering_on,
    is_dynamic,
    is_hybrid,
    is_immutable,
    is_iceberg,
    is_temporary,
    row_count,
    bytes,
    comment,
    created,
    last_altered,
    retention_time as retention_time_in_days
from {database}.information_schema.tables
where table_schema = %s""",
        params=(db_schema,),
    )


@mcp.tool()
def execute_ddl_statement(ddl: str) -> QueryResponse:
    """Execute a DDL Statement to create objects in Snowflake"""
    return sf.execute_query(ddl)


@mcp.tool()
def get_views(database: str, db_schema: str) -> QueryResponse:
    """Get DDL for a View in a Snowflake Database Schema"""
    return sf.execute_query(
        f"""\
select table_name, view_definition
from {database}.information_schema.views
where table_schema = %s""",
        params=(db_schema,),
    )


@mcp.tool()
def get_procedures(database: str, db_schema: str) -> QueryResponse:
    """Get DDL for a Stored Procedure in a Database"""
    return sf.execute_query(
        f"""\
select procedure_name, argument_signature, procedure_definition
from {database}.information_schema.procedures
where procedure_schema = %s""",
        params=(db_schema,),
    )


@mcp.tool()
def execute_select_statement(query: str) -> QueryResponse:
    """Execute a select statement to read data in Snowflake upto 10 rows.

    <warning>
    The select query must always be limited to at most 10 rows and must specify columns.
    </warning>"""
    return sf.execute_query(query)


@mcp.tool()
def execute_select_statement_large(query: str, file: str) -> QueryResponse:
    """Execute a select statement to read data in Snowflake for more than 10 rows.

    <warning>
    This file should not be read and is only intended for the user.
    </warning>"""
    return sf.execute_query(query, outfile=file)


@mcp.tool()
def get_tasks() -> QueryResponse:
    """Get a list and details for Snowflake Tasks. A Snowflake Task is a database trigger."""
    return sf.execute_query("show task")


@mcp.tool()
def get_roles() -> QueryResponse:
    """Get a list of Snowflake Roles."""
    return sf.execute_query("show roles")


@mcp.tool()
def get_roles_hierarchy(role: str = "accountadmin") -> QueryResponse:
    """Get Role Hierarchy for a Snowflake Role."""
    sql = f"""\
with cte as (
  select *
  from snowflake.account_usage.grants_to_roles
  where
    grantee_name ilike '{role}'
    and granted_on = 'ROLE'
    and privilege = 'USAGE'
    and deleted_on is null
  union all
  select g.*
  from snowflake.account_usage.grants_to_roles g 
  inner join cte on g.grantee_name = cte.name
  where
    g.granted_on = 'ROLE'
    and g.privilege = 'USAGE'
    and g.deleted_on is null
)
select
    created_on,
    modified_on,
    privilege,
	granted_on,
	name,
	table_catalog as database,
	table_schema,
	granted_to,
	grantee_name,
	grant_option,
	granted_by
from cte"""
    return sf.execute_query(sql)


@mcp.tool()
def get_roles_grants(role: str = "accountadmin") -> QueryResponse:
    """Get Grants (aka Privilege) for a Snowflake Role."""
    sql = f"""\
with cte as (
  select *
  from snowflake.account_usage.grants_to_roles
  where
    grantee_name ilike '{role}'
    and granted_on = 'ROLE'
    and privilege = 'USAGE'
    and deleted_on is null
  union all
  select g.*
  from snowflake.account_usage.grants_to_roles g 
  inner join cte on g.grantee_name = cte.name
  where
    g.granted_on = 'ROLE'
    and g.privilege = 'USAGE'
    and g.deleted_on is null
)
select
    created_on,
    modified_on,
    privilege,
	granted_on,
	name,
	table_catalog as database,
	table_schema,
	granted_to,
	grantee_name,
	grant_option,
	granted_by
from cte
union
select
    gr.created_on,
    gr.modified_on,
    gr.privilege,
	gr.granted_on,
	gr.name,
	gr.table_catalog as database,
	gr.table_schema,
	gr.granted_to,
	gr.grantee_name,
	gr.grant_option,
	gr.granted_by
from cte as c
left join snowflake.account_usage.grants_to_roles as gr on gr.grantee_name = c.name
where gr.deleted_on is null"""
    return sf.execute_query(sql)


@mcp.tool()
def get_users() -> QueryResponse:
    """Get a list of Users in Snowflake."""
    return sf.execute_query("show users")


@mcp.tool()
def get_user_roles(user: str) -> QueryResponse:
    """Get a list of roles for a Snowflake User."""
    return sf.execute_query(f"show grants to user {user}")


@mcp.tool()
def check_usage_for_object(
    obj: str,
    start_dt: datetime.date = datetime.date(2000, 1, 1),
    end_dt: datetime.date = datetime.date(9999, 12, 31),
) -> QueryResponse:
    """Check the usage of a Snowflake Table, View, Function, Procedure by
    scanning the Snowflake Query History for a Time Period by default the time
    period is all time."""
    start = start_dt.isoformat()
    end = end_dt.isoformat()
    sql = f"""\
select 1
from snowflake.account_usage.query_history
where
    query_text ilike '%{obj}%'
    and end_time between {start} and {end}
limit 1"""
    res = sf.execute_query(sql)

    if res.get("status") == "success":
        return {
            "status": "success",
            "data": "object has been used in the time period",
        }
    else:
        return {
            "status": "success",
            "data": "object has NOT been used in the time period",
        }


if __name__ == "__main__":
    mcp.run()
