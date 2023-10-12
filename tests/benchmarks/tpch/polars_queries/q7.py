from datetime import datetime

import polars as pl

from .utils import read_data


def query():
    nation_ds = read_data("nation")
    customer_ds = read_data("customer")
    line_item_ds = read_data("lineitem")
    orders_ds = read_data("orders")
    supplier_ds = read_data("supplier")

    n1 = nation_ds.filter(pl.col("n_name") == "FRANCE")
    n2 = nation_ds.filter(pl.col("n_name") == "GERMANY")

    var_1 = datetime(1995, 1, 1)
    var_2 = datetime(1996, 12, 31)

    df1 = (
        customer_ds.join(n1, left_on="c_nationkey", right_on="n_nationkey")
        .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .rename({"n_name": "cust_nation"})
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        .join(n2, left_on="s_nationkey", right_on="n_nationkey")
        .rename({"n_name": "supp_nation"})
    )

    df2 = (
        customer_ds.join(n2, left_on="c_nationkey", right_on="n_nationkey")
        .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .rename({"n_name": "cust_nation"})
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
        .join(n1, left_on="s_nationkey", right_on="n_nationkey")
        .rename({"n_name": "supp_nation"})
    )

    return (
        pl.concat([df1, df2])
        .filter(pl.col("l_shipdate").is_between(var_1, var_2))
        .with_columns(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("volume")
        )
        .with_columns(pl.col("l_shipdate").dt.year().alias("l_year"))
        .group_by(["supp_nation", "cust_nation", "l_year"])
        .agg([pl.sum("volume").alias("revenue")])
        .sort(by=["supp_nation", "cust_nation", "l_year"])
    )
