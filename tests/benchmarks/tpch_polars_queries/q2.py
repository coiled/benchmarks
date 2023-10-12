import polars as pl

from .utils import read_data


def query():
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

    return (
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
    )
