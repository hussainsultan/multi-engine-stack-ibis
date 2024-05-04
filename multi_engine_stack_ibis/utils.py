import pathlib

import ibis.expr.operations as ops
import pandas as pd
import snowflake.connector.pandas_tools as pt
from ibis import _
from ibis.backends.pandas.executor import PandasExecutor
from ibis.common.patterns import replace

from .connections import make_raw_snowflake_connection


class CheckpointParquet(ops.relations.Simple):
    path: pathlib.Path


class CreateTableSnowflake(ops.relations.Simple):
    table: str


class MyExecutor(PandasExecutor):
    @classmethod
    def visit(cls, op, *args, **kwargs):
        return super().visit(op, *args, **kwargs)

    @classmethod
    def visit(
        cls,
        op: CheckpointParquet,
        parent,
        path: pathlib.Path,
    ):
        parent.to_parquet(path)
        return parent

    @classmethod
    def visit(
        cls,
        op: CreateTableSnowflake,
        parent,
        table: str,
    ):
        conn = make_raw_snowflake_connection(database="MULTI_ENGINE", schema="PUBLIC")
        conn.cursor().execute("USE MULTI_ENGINE.PUBLIC;")
        pt.write_pandas(
            conn,
            parent,
            table_name=table,
            auto_create_table=True,
            quote_identifiers=False,
        )
        return parent


def replace_unbound(expr, con):
    @replace(ops.UnboundTable)
    def bind_unbound_table(_, backend, **kwargs):
        return ops.DatabaseTable(name=_.name, schema=_.schema, source=backend)

    ctx = {"params": {}, "backend": con}
    return expr.op().replace(bind_unbound_table, context=ctx).to_expr()


def checkpoint_parquet(self, path):
    return CheckpointParquet(self, path).to_expr()


def create_table_snowflake(self, table):
    return CreateTableSnowflake(self, table).to_expr()
