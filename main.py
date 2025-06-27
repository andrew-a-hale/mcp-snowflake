#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["fastmcp", "snowflake-connector-python", "snowflake-core"]
# ///
from utils.sf import Snowflake, QueryResponse

from mcp.server.fastmcp import FastMCP

sf = Snowflake()
mcp = FastMCP("snowflake")


@mcp.tool()
def get_databases() -> QueryResponse:
    """Get List of Snowflake Databases"""
    return sf.execute_query("select * from snowflake.information_schema.databases")


@mcp.tool()
def get_snowflake_schemas(database: str) -> QueryResponse:
    """Get List of Snowflake defined Schemas in a Database"""
    return sf.execute_query(
        f"select * from {database}.information_schema.schemata where created is null"
    )


@mcp.tool()
def get_user_defined_schemas(database: str) -> QueryResponse:
    """Get List of Snowflake User Defined Schemas in a Database"""
    return sf.execute_query(
        f"select * from {database}.information_schema.schemata where created is not null"
    )


@mcp.tool()
def get_tables(database: str, db_schema: str) -> QueryResponse:
    """Get List of Snowflake Tables in a Database Schema"""
    return sf.execute_query(
        f"""\
select *
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
    """Get DDL for a View in a Database"""
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
    The select query must always be limited to at most 10 rows.
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
def get_tasks(database: str, db_schema: str) -> QueryResponse:
    """Get DDL for a Tasks in a Database Schema. A Snowflake Task is a database trigger."""
    queries = []
    queries.append(f"use {database}")
    queries.append(f"use {db_schema}")
    queries.append("show task")
    return sf.execute_query_batch(queries)


@mcp.tool()
def get_roles(database: str, db_schema: str) -> QueryResponse:
    """Get DDL for a Tasks in a Database Schema. A Snowflake Task is a database trigger."""
    queries = []
    queries.append(f"use {database}")
    queries.append(f"use {db_schema}")
    queries.append("show roles")
    return sf.execute_query_batch(queries)


if __name__ == "__main__":
    mcp.run()
