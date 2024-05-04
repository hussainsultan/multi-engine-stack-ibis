import pathlib

import ibis
import ibis.backends.pandas.executor
import ibis.expr.types.relations
import pandas as pd
from ibis import _

from multi_engine_stack_ibis.generator import generate_random_data
from multi_engine_stack_ibis.utils import (MyExecutor, checkpoint_parquet,
                                           create_table_snowflake,
                                           replace_unbound)

if __name__ == "__main__":
    ibis.backends.pandas.executor.PandasExecutor = MyExecutor
    setattr(ibis.expr.types.relations.Table, "checkpoint_parquet", checkpoint_parquet)
    setattr(
        ibis.expr.types.relations.Table,
        "create_table_snowflake",
        create_table_snowflake,
    )
    ibis.set_backend("pandas")

    p_landing = pathlib.Path(f"landing.parquet")
    p_raw = pathlib.Path("raw/orders.parquet")

    for p in (p_landing, p_raw):
        if p.exists():
            p.unlink()

    generate_random_data(p_raw)

    expr = (
        ibis.read_parquet("raw/orders.parquet")
        .mutate(
            row_number=ibis.row_number().over(group_by=[_.order_id], order_by=[_.dt])
        )
        .filter(_.row_number == 0)
        .checkpoint_parquet(p_landing)
        .create_table_snowflake("test_test")
    )
    schema = {
        "level_0": "int64",
        "user_id": "int64",
        "dt": "timestamp",
        "order_id": "string",
        "quantity": "int64",
        "purchase_price": "float64",
        "sku": "string",
        "index": "float64",
        "row_number": "int64",
    }
    first_expr_for = (
        ibis.table(schema, name="orders")
        .mutate(
            row_number=ibis.row_number().over(group_by=[_.order_id], order_by=[_.dt])
        )
        .filter(_.row_number == 0)
    )
    data = pd.read_parquet("raw/orders.parquet")
    backend = ibis.duckdb.connect()
    backend.con.execute("CREATE TABLE orders as SELECT * from data")

    bound = replace_unbound(first_expr_for, backend)
    to_sql = ibis.to_sql(bound)
    result = bound.execute()

    second_expr_for = ibis.table(schema, name="orders")
    backend = ibis.pandas.connect({"orders": result})
    bound = replace_unbound(first_expr_for, backend)
    bound.to_parquet(p_landing)
    result = bound.execute()
