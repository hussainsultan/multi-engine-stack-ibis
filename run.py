import pathlib

import ibis
import ibis.backends.pandas.executor
import ibis.expr.types.relations
from multi_engine_stack_ibis.connections import make_ibis_snowflake_connection
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

    p_staging = pathlib.Path(f"staging.parquet")
    p_landing = pathlib.Path("raw/orders.parquet")

    for p in (p_staging, p_landing):
        if p.exists():
            p.unlink()

    generate_random_data(p_landing)

    expr = (
        ibis.read_parquet("raw/orders.parquet")
        .mutate(
            row_number=ibis.row_number().over(group_by=[_.order_id], order_by=[_.dt])
        )
        .filter(_.row_number == 0)
        .checkpoint_parquet(p_staging)
        .create_table_snowflake("test_test")
    )
    schema = {
        "user_id": "int64",
        "dt": "timestamp",
        "order_id": "string",
        "quantity": "int64",
        "purchase_price": "float64",
        "sku": "string",
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
    duck_backend = ibis.duckdb.connect() #.create_table(data)
    duck_backend.con.execute("CREATE TABLE orders as SELECT * from data")

    bound = replace_unbound(first_expr_for, duck_backend)
    bound.to_parquet(p_staging)
    to_sql = ibis.to_sql(bound)
    result = bound.execute()

    second_expr_for = ibis.table(schema, name="orders")
    pandas_backend = ibis.pandas.connect({"orders": result})
    snow_backend = make_ibis_snowflake_connection(database="MULTI_ENGINE", schema="PUBLIC", warehouse="COMPUTE_WH")
    
    snow_backend.create_table("T_ORDERS", schema=second_expr_for.schema(), temp=True)
    snow_backend.insert("T_ORDERS", pandas_backend.to_pyarrow(second_expr_for))

    third_expr_for = ibis.table(schema, name="T_ORDERS")
    snow_backend.execute(third_expr_for)
