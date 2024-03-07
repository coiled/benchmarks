import os
from datetime import datetime

import pytest

pytestmark = pytest.mark.tpch_nondask

pl = pytest.importorskip("polars")
pytest.importorskip("pyarrow")


def read_data(filename):
    fileglob = os.path.join(filename, "*")

    if filename.startswith("s3://"):
        import boto3

        session = boto3.session.Session()
        credentials = session.get_credentials()
        return pl.scan_parquet(
            fileglob,
            storage_options={
                "aws_access_key_id": credentials.access_key,
                "aws_secret_access_key": credentials.secret_key,
                "aws_region": "us-east-2",
                "aws_session_token": credentials.token,
            },
        )
    else:
        return pl.scan_parquet(fileglob)


def test_query_1(run, restart, dataset_path):
    def _():
        var_1 = datetime(1998, 9, 2)
        q = read_data(dataset_path + "lineitem")
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
                    pl.len().alias("count_order"),
                ],
            )
            .sort(["l_returnflag", "l_linestatus"])
        ).collect(streaming=True)

    run(_)


def test_query_2(run, restart, dataset_path):
    def _():
        var_1 = 15
        var_2 = "BRASS"
        var_3 = "EUROPE"

        region_ds = read_data(dataset_path + "region")
        nation_ds = read_data(dataset_path + "nation")
        supplier_ds = read_data(dataset_path + "supplier")
        part_ds = read_data(dataset_path + "part")
        part_supp_ds = read_data(dataset_path + "partsupp")

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
            .with_columns(pl.col(pl.datatypes.Utf8).str.strip_chars().keep_name())
        ).collect(streaming=True)

    run(_)


def test_query_3(run, restart, dataset_path):
    def _():
        var_1 = var_2 = datetime(1995, 3, 15)
        var_3 = "BUILDING"

        customer_ds = read_data(dataset_path + "customer")
        line_item_ds = read_data(dataset_path + "lineitem")
        orders_ds = read_data(dataset_path + "orders")

        (
            customer_ds.filter(pl.col("c_mktsegment") == var_3)
            .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
            .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
            .filter(pl.col("o_orderdate") < var_2)
            .filter(pl.col("l_shipdate") > var_1)
            .with_columns(
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias(
                    "revenue"
                )
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

    run(_)


def test_query_4(run, restart, dataset_path):
    def _():
        var_1 = datetime(1993, 7, 1)
        var_2 = datetime(1993, 10, 1)

        line_item_ds = read_data(dataset_path + "lineitem")
        orders_ds = read_data(dataset_path + "orders")

        (
            line_item_ds.join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
            .filter(pl.col("o_orderdate").is_between(var_1, var_2, closed="left"))
            .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
            .unique(subset=["o_orderpriority", "l_orderkey"])
            .group_by("o_orderpriority")
            .agg(pl.len().alias("order_count"))
            .sort(by="o_orderpriority")
            .with_columns(pl.col("order_count").cast(pl.datatypes.Int64))
        ).collect(streaming=True)

    run(_)


def test_query_5(run, restart, dataset_path):
    def _():
        var_1 = "ASIA"
        var_2 = datetime(1994, 1, 1)
        var_3 = datetime(1995, 1, 1)

        region_ds = read_data(dataset_path + "region")
        nation_ds = read_data(dataset_path + "nation")
        customer_ds = read_data(dataset_path + "customer")
        line_item_ds = read_data(dataset_path + "lineitem")
        orders_ds = read_data(dataset_path + "orders")
        supplier_ds = read_data(dataset_path + "supplier")

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
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias(
                    "revenue"
                )
            )
            .group_by("n_name")
            .agg([pl.sum("revenue")])
            .sort(by="revenue", descending=True)
        ).collect(streaming=True)

    run(_)


def test_query_6(run, restart, dataset_path):
    def _():
        var_1 = datetime(1994, 1, 1)
        var_2 = datetime(1995, 1, 1)
        var_3 = 24

        line_item_ds = read_data(dataset_path + "lineitem")

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

    run(_)


def test_query_7(run, restart, dataset_path):
    def _():
        nation_ds = read_data(dataset_path + "nation")
        customer_ds = read_data(dataset_path + "customer")
        line_item_ds = read_data(dataset_path + "lineitem")
        orders_ds = read_data(dataset_path + "orders")
        supplier_ds = read_data(dataset_path + "supplier")

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

    run(_)


def test_query_8(run, restart, dataset_path):
    def _():
        part_ds = read_data(dataset_path + "part")
        supplier_ds = read_data(dataset_path + "supplier")
        line_item_ds = read_data(dataset_path + "lineitem")
        orders_ds = read_data(dataset_path + "orders")
        customer_ds = read_data(dataset_path + "customer")
        nation_ds = read_data(dataset_path + "nation")
        region_ds = read_data(dataset_path + "region")

        n1 = nation_ds.select(["n_nationkey", "n_regionkey"])
        n2 = nation_ds.clone().select(["n_nationkey", "n_name"])

        q_final = (
            part_ds.join(line_item_ds, left_on="p_partkey", right_on="l_partkey")
            .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
            .join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
            .join(customer_ds, left_on="o_custkey", right_on="c_custkey")
            .join(n1, left_on="c_nationkey", right_on="n_nationkey")
            .join(region_ds, left_on="n_regionkey", right_on="r_regionkey")
            .filter(pl.col("r_name") == "AMERICA")
            .join(n2, left_on="s_nationkey", right_on="n_nationkey")
            .filter(
                pl.col("o_orderdate").is_between(
                    datetime(1995, 1, 1), datetime(1996, 12, 31)
                )
            )
            .filter(pl.col("p_type") == "ECONOMY ANODIZED STEEL")
            .select(
                [
                    pl.col("o_orderdate").dt.year().alias("o_year"),
                    (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias(
                        "volume"
                    ),
                    pl.col("n_name").alias("nation"),
                ]
            )
            .with_columns(
                pl.when(pl.col("nation") == "BRAZIL")
                .then(pl.col("volume"))
                .otherwise(0)
                .alias("_tmp")
            )
            .group_by("o_year")
            .agg((pl.sum("_tmp") / pl.sum("volume")).round(2).alias("mkt_share"))
            .sort("o_year")
        )
        q_final.collect(streaming=True)

    run(_)


def test_query_9(run, restart, dataset_path):
    def _():
        part_ds = read_data(dataset_path + "part")
        supplier_ds = read_data(dataset_path + "supplier")
        line_item_ds = read_data(dataset_path + "lineitem")
        part_supp_ds = read_data(dataset_path + "partsupp")
        orders_ds = read_data(dataset_path + "orders")
        nation_ds = read_data(dataset_path + "nation")

        q_final = (
            line_item_ds.join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
            .join(
                part_supp_ds,
                left_on=["l_suppkey", "l_partkey"],
                right_on=["ps_suppkey", "ps_partkey"],
            )
            .join(part_ds, left_on="l_partkey", right_on="p_partkey")
            .join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
            .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
            .filter(pl.col("p_name").str.contains("green"))
            .select(
                [
                    pl.col("n_name").alias("nation"),
                    pl.col("o_orderdate").dt.year().alias("o_year"),
                    (
                        pl.col("l_extendedprice") * (1 - pl.col("l_discount"))
                        - pl.col("ps_supplycost") * pl.col("l_quantity")
                    ).alias("amount"),
                ]
            )
            .group_by(["nation", "o_year"])
            .agg(pl.sum("amount").round(2).alias("sum_profit"))
            .sort(by=["nation", "o_year"], descending=[False, True])
        )
        q_final.collect(streaming=True)

    run(_)


def test_query_10(run, restart, dataset_path):
    def _():
        customer_ds = read_data(dataset_path + "customer")
        orders_ds = read_data(dataset_path + "orders")
        line_item_ds = read_data(dataset_path + "lineitem")
        nation_ds = read_data(dataset_path + "nation")

        var_1 = datetime(1993, 10, 1)
        var_2 = datetime(1994, 1, 1)

        q_final = (
            customer_ds.join(orders_ds, left_on="c_custkey", right_on="o_custkey")
            .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
            .join(nation_ds, left_on="c_nationkey", right_on="n_nationkey")
            .filter(pl.col("o_orderdate").is_between(var_1, var_2, closed="left"))
            .filter(pl.col("l_returnflag") == "R")
            .group_by(
                [
                    "c_custkey",
                    "c_name",
                    "c_acctbal",
                    "c_phone",
                    "n_name",
                    "c_address",
                    "c_comment",
                ]
            )
            .agg(
                [
                    (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
                    .sum()
                    .round(2)
                    .alias("revenue")
                ]
            )
            .with_columns(
                pl.col("c_address").str.strip_chars(),
                pl.col("c_comment").str.strip_chars(),
            )
            .select(
                [
                    "c_custkey",
                    "c_name",
                    "revenue",
                    "c_acctbal",
                    "n_name",
                    "c_address",
                    "c_phone",
                    "c_comment",
                ]
            )
            .sort(by="revenue", descending=True)
            .limit(20)
        )
        q_final.collect(streaming=True)

    run(_)


def test_query_11(run, restart, dataset_path, scale):
    def _():
        supplier_ds = read_data(dataset_path + "supplier")
        part_supp_ds = read_data(dataset_path + "partsupp")
        nation_ds = read_data(dataset_path + "nation")

        var_1 = "GERMANY"
        var_2 = 0.0001 / scale

        res_1 = (
            part_supp_ds.join(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey")
            .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
            .filter(pl.col("n_name") == var_1)
        )
        res_2 = res_1.select(
            (pl.col("ps_supplycost") * pl.col("ps_availqty"))
            .sum()
            .round(2)
            .alias("tmp")
            * var_2
        ).with_columns(pl.lit(1).alias("lit"))

        q_final = (
            res_1.group_by("ps_partkey")
            .agg(
                (pl.col("ps_supplycost") * pl.col("ps_availqty"))
                .sum()
                .round(2)
                .alias("value")
            )
            .with_columns(pl.lit(1).alias("lit"))
            .join(res_2, on="lit")
            .filter(pl.col("value") > pl.col("tmp"))
            .select(["ps_partkey", "value"])
            .sort("value", descending=True)
        )
        q_final.collect(streaming=True)

    run(_)


def test_query_12(run, restart, dataset_path):
    def _():
        line_item_ds = read_data(dataset_path + "lineitem")
        orders_ds = read_data(dataset_path + "orders")

        var_1 = "MAIL"
        var_2 = "SHIP"
        var_3 = datetime(1994, 1, 1)
        var_4 = datetime(1995, 1, 1)

        q_final = (
            orders_ds.join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
            .filter(pl.col("l_shipmode").is_in([var_1, var_2]))
            .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
            .filter(pl.col("l_shipdate") < pl.col("l_commitdate"))
            .filter(pl.col("l_receiptdate").is_between(var_3, var_4, closed="left"))
            .with_columns(
                [
                    pl.when(pl.col("o_orderpriority").is_in(["1-URGENT", "2-HIGH"]))
                    .then(1)
                    .otherwise(0)
                    .alias("high_line_count"),
                    pl.when(
                        pl.col("o_orderpriority").is_in(["1-URGENT", "2-HIGH"]).not_()
                    )
                    .then(1)
                    .otherwise(0)
                    .alias("low_line_count"),
                ]
            )
            .group_by("l_shipmode")
            .agg([pl.col("high_line_count").sum(), pl.col("low_line_count").sum()])
            .sort("l_shipmode")
        )
        q_final.collect(streaming=True)

    run(_)


def test_query_13(run, restart, dataset_path):
    def _():
        var_1 = "special"
        var_2 = "requests"

        customer_ds = read_data(dataset_path + "customer")
        orders_ds = read_data(dataset_path + "orders").filter(
            pl.col("o_comment").str.contains(f"{var_1}.*{var_2}").not_()
        )
        q_final = (
            customer_ds.join(
                orders_ds, left_on="c_custkey", right_on="o_custkey", how="left"
            )
            .group_by("c_custkey")
            .agg(pl.col("o_orderkey").len().alias("c_count"))
            .group_by("c_count")
            .len()
            .select([pl.col("c_count"), pl.col("len").alias("custdist")])
            .sort(["custdist", "c_count"], descending=[True, True])
        )
        q_final.collect(streaming=True)

    run(_)


def test_query_14(run, restart, dataset_path):
    def _():
        line_item_ds = read_data(dataset_path + "lineitem")
        part_ds = read_data(dataset_path + "part")

        var_1 = datetime(1995, 9, 1)
        var_2 = datetime(1995, 10, 1)

        q_final = (
            line_item_ds.join(part_ds, left_on="l_partkey", right_on="p_partkey")
            .filter(pl.col("l_shipdate").is_between(var_1, var_2, closed="left"))
            .select(
                (
                    100.00
                    * pl.when(pl.col("p_type").str.contains("PROMO*"))
                    .then(pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
                    .otherwise(0)
                    .sum()
                    / (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).sum()
                )
                .round(2)
                .alias("promo_revenue")
            )
        )
        q_final.collect(streaming=True)

    run(_)


def test_query_15(run, restart, dataset_path):
    def _():
        line_item_ds = read_data(dataset_path + "lineitem")
        supplier_ds = read_data(dataset_path + "supplier")

        var_1 = datetime(1996, 1, 1)
        var_2 = datetime(1996, 4, 1)

        revenue_ds = (
            line_item_ds.filter(
                pl.col("l_shipdate").is_between(var_1, var_2, closed="left")
            )
            .group_by("l_suppkey")
            .agg(
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
                .sum()
                .alias("total_revenue")
            )
            .select([pl.col("l_suppkey").alias("supplier_no"), pl.col("total_revenue")])
        )

        q_final = (
            supplier_ds.join(revenue_ds, left_on="s_suppkey", right_on="supplier_no")
            .filter(pl.col("total_revenue") == pl.col("total_revenue").max())
            .with_columns(pl.col("total_revenue").round(2))
            .select(["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"])
            .sort("s_suppkey")
        )
        q_final.collect(streaming=True)

    run(_)


def test_query_16(run, restart, dataset_path):
    def _():
        part_supp_ds = read_data(dataset_path + "partsupp")
        part_ds = read_data(dataset_path + "part")
        supplier_ds = (
            read_data(dataset_path + "supplier")
            .filter(pl.col("s_comment").str.contains(".*Customer.*Complaints.*"))
            .select(pl.col("s_suppkey"), pl.col("s_suppkey").alias("ps_suppkey"))
        )

        var_1 = "Brand#45"

        q_final = (
            part_ds.join(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
            .filter(pl.col("p_brand") != var_1)
            .filter(pl.col("p_type").str.contains("MEDIUM POLISHED*").not_())
            .filter(pl.col("p_size").is_in([49, 14, 23, 45, 19, 3, 36, 9]))
            .join(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey", how="left")
            .filter(pl.col("ps_suppkey_right").is_null())
            .group_by(["p_brand", "p_type", "p_size"])
            .agg([pl.col("ps_suppkey").n_unique().alias("supplier_cnt")])
            .sort(
                by=["supplier_cnt", "p_brand", "p_type", "p_size"],
                descending=[True, False, False, False],
            )
        )
        q_final.collect(streaming=True)

    run(_)


def test_query_17(run, restart, dataset_path):
    def _():
        var_1 = "Brand#23"
        var_2 = "MED BOX"

        line_item_ds = read_data(dataset_path + "lineitem")
        part_ds = read_data(dataset_path + "part")

        res_1 = (
            part_ds.filter(pl.col("p_brand") == var_1)
            .filter(pl.col("p_container") == var_2)
            .join(line_item_ds, how="left", left_on="p_partkey", right_on="l_partkey")
        ).cache()

        q_final = (
            res_1.group_by("p_partkey")
            .agg((0.2 * pl.col("l_quantity").mean()).alias("avg_quantity"))
            .select([pl.col("p_partkey").alias("key"), pl.col("avg_quantity")])
            .join(res_1, left_on="key", right_on="p_partkey")
            .filter(pl.col("l_quantity") < pl.col("avg_quantity"))
            .select(
                (pl.col("l_extendedprice").sum() / 7.0).round(2).alias("avg_yearly")
            )
        )
        q_final.collect(streaming=True)

    run(_)


def test_query_18(run, restart, dataset_path):
    def _():
        customer_ds = read_data(dataset_path + "customer")
        line_item_ds = read_data(dataset_path + "lineitem")
        orders_ds = read_data(dataset_path + "orders")

        var_1 = 300

        q_final = (
            line_item_ds.group_by("l_orderkey")
            .agg(pl.col("l_quantity").sum().alias("sum_quantity"))
            .filter(pl.col("sum_quantity") > var_1)
            .select([pl.col("l_orderkey").alias("key"), pl.col("sum_quantity")])
            .join(orders_ds, left_on="key", right_on="o_orderkey")
            .join(line_item_ds, left_on="key", right_on="l_orderkey")
            .join(customer_ds, left_on="o_custkey", right_on="c_custkey")
            .group_by("c_name", "o_custkey", "key", "o_orderdate", "o_totalprice")
            .agg(pl.col("l_quantity").sum().alias("col6"))
            .select(
                [
                    pl.col("c_name"),
                    pl.col("o_custkey").alias("c_custkey"),
                    pl.col("key").alias("o_orderkey"),
                    pl.col("o_orderdate").alias("o_orderdat"),
                    pl.col("o_totalprice"),
                    pl.col("col6"),
                ]
            )
            .sort(["o_totalprice", "o_orderdat"], descending=[True, False])
            .limit(100)
        )
        q_final.collect(streaming=True)

    run(_)


def test_query_19(run, restart, dataset_path):
    def _():
        line_item_ds = read_data(dataset_path + "lineitem")
        part_ds = read_data(dataset_path + "part")

        q_final = (
            part_ds.join(line_item_ds, left_on="p_partkey", right_on="l_partkey")
            .filter(pl.col("l_shipmode").is_in(["AIR", "AIR REG"]))
            .filter(pl.col("l_shipinstruct") == "DELIVER IN PERSON")
            .filter(
                (
                    (pl.col("p_brand") == "Brand#12")
                    & pl.col("p_container").is_in(
                        ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]
                    )
                    & (pl.col("l_quantity").is_between(1, 11))
                    & (pl.col("p_size").is_between(1, 5))
                )
                | (
                    (pl.col("p_brand") == "Brand#23")
                    & pl.col("p_container").is_in(
                        ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]
                    )
                    & (pl.col("l_quantity").is_between(10, 20))
                    & (pl.col("p_size").is_between(1, 10))
                )
                | (
                    (pl.col("p_brand") == "Brand#34")
                    & pl.col("p_container").is_in(
                        ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]
                    )
                    & (pl.col("l_quantity").is_between(20, 30))
                    & (pl.col("p_size").is_between(1, 15))
                )
            )
            .select(
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
                .sum()
                .round(2)
                .alias("revenue")
            )
        )
        q_final.collect(streaming=True)

    run(_)


def test_query_20(run, restart, dataset_path):
    def _():
        line_item_ds = read_data(dataset_path + "lineitem")
        nation_ds = read_data(dataset_path + "nation")
        supplier_ds = read_data(dataset_path + "supplier")
        part_ds = read_data(dataset_path + "part")
        part_supp_ds = read_data(dataset_path + "partsupp")

        var_1 = datetime(1994, 1, 1)
        var_2 = datetime(1995, 1, 1)
        var_3 = "CANADA"
        var_4 = "forest"

        res_1 = (
            line_item_ds.filter(
                pl.col("l_shipdate").is_between(var_1, var_2, closed="left")
            )
            .group_by("l_partkey", "l_suppkey")
            .agg((pl.col("l_quantity").sum() * 0.5).alias("sum_quantity"))
        )
        res_2 = nation_ds.filter(pl.col("n_name") == var_3)
        res_3 = supplier_ds.join(res_2, left_on="s_nationkey", right_on="n_nationkey")

        q_final = (
            part_ds.filter(pl.col("p_name").str.starts_with(var_4))
            .select(pl.col("p_partkey").unique())
            .join(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
            .join(
                res_1,
                left_on=["ps_suppkey", "p_partkey"],
                right_on=["l_suppkey", "l_partkey"],
            )
            .filter(pl.col("ps_availqty") > pl.col("sum_quantity"))
            .select(pl.col("ps_suppkey").unique())
            .join(res_3, left_on="ps_suppkey", right_on="s_suppkey")
            .with_columns(pl.col("s_address").str.strip_chars())
            .select(["s_name", "s_address"])
            .sort("s_name")
        )
        q_final.collect(streaming=True)

    run(_)


def test_query_21(run, restart, dataset_path):
    def _():
        line_item_ds = read_data(dataset_path + "lineitem")
        supplier_ds = read_data(dataset_path + "supplier")
        nation_ds = read_data(dataset_path + "nation")
        orders_ds = read_data(dataset_path + "orders")

        var_1 = "SAUDI ARABIA"

        res_1 = (
            line_item_ds.group_by("l_orderkey")
            .agg(pl.col("l_suppkey").n_unique().alias("nunique_col"))
            .filter(pl.col("nunique_col") > 1)
            .join(
                line_item_ds.filter(pl.col("l_receiptdate") > pl.col("l_commitdate")),
                on="l_orderkey",
            )
        ).cache()

        q_final = (
            res_1.group_by("l_orderkey")
            .agg(pl.col("l_suppkey").n_unique().alias("nunique_col"))
            .join(res_1, on="l_orderkey")
            .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
            .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
            .join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
            .filter(pl.col("nunique_col") == 1)
            .filter(pl.col("n_name") == var_1)
            .filter(pl.col("o_orderstatus") == "F")
            .group_by("s_name")
            .agg(pl.len().alias("numwait"))
            .sort(by=["numwait", "s_name"], descending=[True, False])
            .limit(100)
        )
        q_final.collect(streaming=True)

    run(_)


def test_query_22(run, restart, dataset_path):
    def _():
        orders_ds = read_data(dataset_path + "orders")
        customer_ds = read_data(dataset_path + "customer")

        res_1 = (
            customer_ds.with_columns(
                pl.col("c_phone").str.slice(0, 2).alias("cntrycode")
            )
            .filter(pl.col("cntrycode").str.contains("13|31|23|29|30|18|17"))
            .select(["c_acctbal", "c_custkey", "cntrycode"])
        )

        res_2 = (
            res_1.filter(pl.col("c_acctbal") > 0.0)
            .select(pl.col("c_acctbal").mean().alias("avg_acctbal"))
            .with_columns(pl.lit(1).alias("lit"))
        )

        res_3 = orders_ds.select(pl.col("o_custkey").unique()).with_columns(
            pl.col("o_custkey").alias("c_custkey")
        )

        q_final = (
            res_1.join(res_3, on="c_custkey", how="left")
            .filter(pl.col("o_custkey").is_null())
            .with_columns(pl.lit(1).alias("lit"))
            .join(res_2, on="lit")
            .filter(pl.col("c_acctbal") > pl.col("avg_acctbal"))
            .group_by("cntrycode")
            .agg(
                [
                    pl.col("c_acctbal").len().alias("numcust"),
                    pl.col("c_acctbal").sum().round(2).alias("totacctbal"),
                ]
            )
            .sort("cntrycode")
        )
        q_final.collect(streaming=True)

    run(_)
