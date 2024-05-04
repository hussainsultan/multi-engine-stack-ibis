import os

import ibis
import snowflake.connector

DEFAULT_ROLE = os.environ.get("SNOWFLAKE_ROLE")
DEFAULT_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")


def make_credential_defaults():
    return {
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "user": os.environ.get("SNOWFLAKE_USER"),
        "password": os.environ.get("SNOWFLAKE_PASSWORD"),
    }


def execute_statement(con, statement):
    (((resp,), *rest0), *rest1) = con.con.execute_string(statement)
    if rest0 or rest1 or (resp != "Statement executed successfully."):
        raise ValueError


def make_ibis_snowflake_connection(
    database, schema, *, role=DEFAULT_ROLE, warehouse=DEFAULT_WAREHOUSE, **kwargs
):
    con = ibis.snowflake.connect(
        database=f"{database}/{schema}",
        create_object_udfs=False,
        **{
            **make_credential_defaults(),
            **kwargs,
        },
    )
    if role:
        execute_statement(con, f"USE ROLE {role}")
    if warehouse:
        execute_statement(con, f"USE WAREHOUSE {warehouse}")
    return con


def make_raw_snowflake_connection(database, schema, **kwargs):
    return snowflake.connector.connect(
        database=database,
        schema=schema,
        **{
            **make_credential_defaults(),
            **kwargs,
        },
    )
