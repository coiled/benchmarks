from datetime import datetime

import polars as pl
from pyarrow.dataset import dataset

from tests.benchmarks.tpch.conftest import DATASETS, ENABLED_DATASET, coiled_function


def read_data(name, source=None):
    pyarrow_dataset = dataset(
        source=source or f"{DATASETS[ENABLED_DATASET]}{name}/", format="parquet"
    )
    return pl.scan_pyarrow_dataset(pyarrow_dataset)


@coiled_function()
def test_query_1(restart):
    var_1 = datetime(1998, 9, 2)
    q = read_data("lineitem")
    (
        q.filter(pl.col("l_shipdate") <= var_1)
        .group_by(["l_returnflag", "l_linestatus"])
        .agg(
            [
                pl.sum("l_quantity").alias("sum_qty"),
                pl.sum("l_extendedprice").alias("sum_base_price"),
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
                .sum()
                .alias("sum_disc_price"),
                (
                    pl.col("l_extendedprice")
                    * (1.0 - pl.col("l_discount"))
                    * (1.0 + pl.col("l_tax"))
                )
                .sum()
                .alias("sum_charge"),
                pl.mean("l_quantity").alias("avg_qty"),
                pl.mean("l_extendedprice").alias("avg_price"),
                pl.mean("l_discount").alias("avg_disc"),
                pl.count().alias("count_order"),
            ],
        )
        .sort(["l_returnflag", "l_linestatus"])
    ).collect(streaming=True)


@coiled_function()
def test_query_2(restart):
    var_1 = 15
    var_2 = "BRASS"
    var_3 = "EUROPE"

    region_ds = read_data("region")
    nation_ds = read_data("nation")
    supplier_ds = read_data("supplier")
    part_ds = read_data("part")
    part_supp_ds = read_data("partsupp")

    result_q1 = (
        part_ds.join(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
        .join(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey")
        .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
        .join(region_ds, left_on="n_regionkey", right_on="r_regionkey")
        .filter(pl.col("p_size") == var_1)
        .filter(pl.col("p_type").str.ends_with(var_2))
        .filter(pl.col("r_name") == var_3)
    ).cache()

    final_cols = [
        "s_acctbal",
        "s_name",
        "n_name",
        "p_partkey",
        "p_mfgr",
        "s_address",
        "s_phone",
        "s_comment",
    ]

    (
        result_q1.group_by("p_partkey")
        .agg(pl.min("ps_supplycost").alias("ps_supplycost"))
        .join(
            result_q1,
            left_on=["p_partkey", "ps_supplycost"],
            right_on=["p_partkey", "ps_supplycost"],
        )
        .select(final_cols)
        .sort(
            by=["s_acctbal", "n_name", "s_name", "p_partkey"],
            descending=[True, False, False, False],
        )
        .limit(100)
        .with_columns(pl.col(pl.datatypes.Utf8).str.strip().keep_name())
    ).collect(streaming=True)


@coiled_function()
def test_query_3(restart):
    var_1 = var_2 = datetime(1995, 3, 15)
    var_3 = "BUILDING"

    customer_ds = read_data("customer")
    line_item_ds = read_data("lineitem")
    orders_ds = read_data("orders")

    (
        customer_ds.filter(pl.col("c_mktsegment") == var_3)
        .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .filter(pl.col("o_orderdate") < var_2)
        .filter(pl.col("l_shipdate") > var_1)
        .with_columns(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")
        )
        .group_by(["o_orderkey", "o_orderdate", "o_shippriority"])
        .agg([pl.sum("revenue")])
        .select(
            [
                pl.col("o_orderkey").alias("l_orderkey"),
                "revenue",
                "o_orderdate",
                "o_shippriority",
            ]
        )
        .sort(by=["revenue", "o_orderdate"], descending=[True, False])
        .limit(10)
    ).collect(streaming=True)


@coiled_function()
def test_query_4(restart):
    var_1 = datetime(1993, 7, 1)
    var_2 = datetime(1993, 10, 1)

    line_item_ds = read_data("lineitem")
    orders_ds = read_data("orders")

    (
        line_item_ds.join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
        .filter(pl.col("o_orderdate").is_between(var_1, var_2, closed="left"))
        .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
        .unique(subset=["o_orderpriority", "l_orderkey"])
        .group_by("o_orderpriority")
        .agg(pl.count().alias("order_count"))
        .sort(by="o_orderpriority")
        .with_columns(pl.col("order_count").cast(pl.datatypes.Int64))
    ).collect(streaming=True)


@coiled_function()
def test_query_5(restart):
    var_1 = "ASIA"
    var_2 = datetime(1994, 1, 1)
    var_3 = datetime(1995, 1, 1)

    region_ds = read_data("region")
    nation_ds = read_data("nation")
    customer_ds = read_data("customer")
    line_item_ds = read_data("lineitem")
    orders_ds = read_data("orders")
    supplier_ds = read_data("supplier")

    (
        region_ds.join(nation_ds, left_on="r_regionkey", right_on="n_regionkey")
        .join(customer_ds, left_on="n_nationkey", right_on="c_nationkey")
        .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .join(
            supplier_ds,
            left_on=["l_suppkey", "n_nationkey"],
            right_on=["s_suppkey", "s_nationkey"],
        )
        .filter(pl.col("r_name") == var_1)
        .filter(pl.col("o_orderdate").is_between(var_2, var_3, closed="left"))
        .with_columns(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")
        )
        .group_by("n_name")
        .agg([pl.sum("revenue")])
        .sort(by="revenue", descending=True)
    ).collect(streaming=True)


@coiled_function()
def test_query_6(restart):
    var_1 = datetime(1994, 1, 1)
    var_2 = datetime(1995, 1, 1)
    var_3 = 24

    line_item_ds = read_data("lineitem")

    (
        line_item_ds.filter(
            pl.col("l_shipdate").is_between(var_1, var_2, closed="left")
        )
        .filter(pl.col("l_discount").is_between(0.05, 0.07))
        .filter(pl.col("l_quantity") < var_3)
        .with_columns(
            (pl.col("l_extendedprice") * pl.col("l_discount")).alias("revenue")
        )
        .select(pl.sum("revenue").alias("revenue"))
    ).collect(streaming=True)


@coiled_function()
def test_query_7(restart):
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

    (
        pl.concat([df1, df2])
        .filter(pl.col("l_shipdate").is_between(var_1, var_2))
        .with_columns(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("volume")
        )
        .with_columns(pl.col("l_shipdate").dt.year().alias("l_year"))
        .group_by(["supp_nation", "cust_nation", "l_year"])
        .agg([pl.sum("volume").alias("revenue")])
        .sort(by=["supp_nation", "cust_nation", "l_year"])
    ).collect(streaming=True)
