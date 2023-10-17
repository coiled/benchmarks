from datetime import datetime

import dask_expr as dd

from .conftest import DATASETS, ENABLED_DATASET


def read_data(filename):
    path = DATASETS[ENABLED_DATASET] + filename + "/"
    return dd.read_parquet(path, engine="pyarrow")


def test_query_1(tpch_client):
    VAR1 = datetime(1998, 9, 2)
    lineitem_ds = read_data("lineitem")

    lineitem_filtered = lineitem_ds[lineitem_ds.l_shipdate <= VAR1]
    lineitem_filtered["sum_qty"] = lineitem_filtered.l_quantity
    lineitem_filtered["sum_base_price"] = lineitem_filtered.l_extendedprice
    lineitem_filtered["avg_qty"] = lineitem_filtered.l_quantity
    lineitem_filtered["avg_price"] = lineitem_filtered.l_extendedprice
    lineitem_filtered["sum_disc_price"] = lineitem_filtered.l_extendedprice * (
        1 - lineitem_filtered.l_discount
    )
    lineitem_filtered["sum_charge"] = (
        lineitem_filtered.l_extendedprice
        * (1 - lineitem_filtered.l_discount)
        * (1 + lineitem_filtered.l_tax)
    )
    lineitem_filtered["avg_disc"] = lineitem_filtered.l_discount
    lineitem_filtered["count_order"] = lineitem_filtered.l_orderkey
    gb = lineitem_filtered.groupby(["l_returnflag", "l_linestatus"])

    total = gb.agg(
        {
            "sum_qty": "sum",
            "sum_base_price": "sum",
            "sum_disc_price": "sum",
            "sum_charge": "sum",
            "avg_qty": "mean",
            "avg_price": "mean",
            "avg_disc": "mean",
            "count_order": "count",
        }
    )

    total.reset_index().sort_values(["l_returnflag", "l_linestatus"]).compute()


#
# def test_query_2(tpch_client):
#     var1 = 15
#     var2 = "BRASS"
#     var3 = "EUROPE"
#
#     region_ds = read_data("region")
#     nation_filtered = read_data("nation")
#     supplier_filtered = read_data("supplier")
#     part_filtered = read_data("part")
#     partsupp_filtered = read_data("partsupp")
#
#     region_filtered = region_ds[(region_ds["r_name"] == var3)]
#     r_n_merged = nation_filtered.merge(
#         region_filtered, left_on="n_regionkey", right_on="r_regionkey", how="inner"
#     )
#     s_r_n_merged = r_n_merged.merge(
#         supplier_filtered,
#         left_on="n_nationkey",
#         right_on="s_nationkey",
#         how="inner",
#     )
#     ps_s_r_n_merged = s_r_n_merged.merge(
#         partsupp_filtered, left_on="s_suppkey", right_on="ps_suppkey", how="inner"
#     )
#     part_filtered = part_filtered[
#         (part_filtered["p_size"] == var1)
#         & (part_filtered["p_type"].astype(str).str.endswith(var2))
#     ]
#     merged_df = part_filtered.merge(
#         ps_s_r_n_merged, left_on="p_partkey", right_on="ps_partkey", how="inner"
#     )
#     min_values = merged_df.groupby("p_partkey")["ps_supplycost"].min().reset_index()
#     min_values.columns = ["P_PARTKEY_CPY", "MIN_SUPPLYCOST"]
#     merged_df = merged_df.merge(
#         min_values,
#         left_on=["p_partkey", "ps_supplycost"],
#         right_on=["P_PARTKEY_CPY", "MIN_SUPPLYCOST"],
#         how="inner",
#     )
#     merged_df[
#         [
#             "s_acctbal",
#             "s_name",
#             "n_name",
#             "p_partkey",
#             "p_mfgr",
#             "s_address",
#             "s_phone",
#             "s_comment",
#         ]
#     ].compute().sort_values(
#         by=[
#             "s_acctbal",
#             "n_name",
#             "s_name",
#             "p_partkey",
#         ],
#         ascending=[
#             False,
#             True,
#             True,
#             True,
#         ],
#     ).head(
#         100
#     )
#
#
# def test_query_3(tpch_client):
#     var1 = datetime.strptime("1995-03-15", "%Y-%m-%d")
#     var2 = "BUILDING"
#
#     lineitem_ds = read_data("lineitem")
#     orders_ds = read_data("orders")
#     cutomer_ds = read_data("customer")
#
#     lsel = lineitem_ds.l_shipdate > var1
#     osel = orders_ds.o_orderdate < var1
#     csel = cutomer_ds.c_mktsegment == var2
#     flineitem = lineitem_ds[lsel]
#     forders = orders_ds[osel]
#     fcustomer = cutomer_ds[csel]
#     jn1 = fcustomer.merge(forders, left_on="c_custkey", right_on="o_custkey")
#     jn2 = jn1.merge(flineitem, left_on="o_orderkey", right_on="l_orderkey")
#     jn2["revenue"] = jn2.l_extendedprice * (1 - jn2.l_discount)
#     total = jn2.groupby(["l_orderkey", "o_orderdate", "o_shippriority"])[
#         "revenue"
#     ].sum()
#     total.reset_index().sort_values(["revenue"], ascending=False).head(10)[
#         ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"]
#     ]
#
#
# def test_query_4(tpch_client):
#     date1 = datetime.strptime("1993-10-01", "%Y-%m-%d")
#     date2 = datetime.strptime("1993-07-01", "%Y-%m-%d")
#
#     line_item_ds = read_data("lineitem")
#     orders_ds = read_data("orders")
#
#     lsel = line_item_ds.l_commitdate < line_item_ds.l_receiptdate
#     osel = (orders_ds.o_orderdate < date1) & (orders_ds.o_orderdate >= date2)
#     flineitem = line_item_ds[lsel]
#     forders = orders_ds[osel]
#     jn = forders.merge(
#         flineitem, left_on="o_orderkey", right_on="l_orderkey"
#     ).drop_duplicates(subset=["o_orderkey"])
#     result_df = (
#         jn.groupby("o_orderpriority")["o_orderkey"]
#         .count()
#         .reset_index()
#         .sort_values(["o_orderpriority"])
#     )
#     result_df.rename(columns={"o_orderkey": "order_count"}).compute()
#
#
# def test_query_5(tpch_client):
#     date1 = datetime.strptime("1994-01-01", "%Y-%m-%d")
#     date2 = datetime.strptime("1995-01-01", "%Y-%m-%d")
#
#     region_ds = read_data("region")
#     nation_ds = read_data("nation")
#     customer_ds = read_data("customer")
#     line_item_ds = read_data("lineitem")
#     orders_ds = read_data("orders")
#     supplier_ds = read_data("supplier")
#
#     rsel = region_ds.r_name == "ASIA"
#     osel = (orders_ds.o_orderdate >= date1) & (orders_ds.o_orderdate < date2)
#     forders = orders_ds[osel]
#     fregion = region_ds[rsel]
#     jn1 = fregion.merge(nation_ds, left_on="r_regionkey", right_on="n_regionkey")
#     jn2 = jn1.merge(customer_ds, left_on="n_nationkey", right_on="c_nationkey")
#     jn3 = jn2.merge(forders, left_on="c_custkey", right_on="o_custkey")
#     jn4 = jn3.merge(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
#     jn5 = supplier_ds.merge(
#         jn4,
#         left_on=["s_suppkey", "s_nationkey"],
#         right_on=["l_suppkey", "n_nationkey"],
#     )
#     jn5["revenue"] = jn5.l_extendedprice * (1.0 - jn5.l_discount)
#     gb = jn5.groupby("n_name")["revenue"].sum()
#     gb.reset_index().sort_values("revenue", ascending=False).compute()
#
#
# def test_query_6(tpch_client):
#     date1 = datetime.strptime("1994-01-01", "%Y-%m-%d")
#     date2 = datetime.strptime("1995-01-01", "%Y-%m-%d")
#     var3 = 24
#
#     line_item_ds = read_data("lineitem")
#
#     sel = (
#         (line_item_ds.l_shipdate >= date1)
#         & (line_item_ds.l_shipdate < date2)
#         & (line_item_ds.l_discount >= 0.05)
#         & (line_item_ds.l_discount <= 0.07)
#         & (line_item_ds.l_quantity < var3)
#     )
#
#     flineitem = line_item_ds[sel]
#     (flineitem.l_extendedprice * flineitem.l_discount).sum().compute()
#
#
# def test_query_7(tpch_client):
#     var1 = datetime.strptime("1995-01-01", "%Y-%m-%d")
#     var2 = datetime.strptime("1997-01-01", "%Y-%m-%d")
#
#     nation_ds = read_data("nation")
#     customer_ds = read_data("customer")
#     line_item_ds = read_data("lineitem")
#     orders_ds = read_data("orders")
#     supplier_ds = read_data("supplier")
#
#     lineitem_filtered = line_item_ds[
#         (line_item_ds["l_shipdate"] >= var1) & (line_item_ds["l_shipdate"] < var2)
#     ]
#     lineitem_filtered["l_year"] = 1  # lineitem_filtered["l_shipdate"].dt.year
#     lineitem_filtered["revenue"] = lineitem_filtered["l_extendedprice"] * (
#         1.0 - lineitem_filtered["l_discount"]
#     )
#
#     supplier_filtered = supplier_ds
#     orders_filtered = orders_ds
#     customer_filtered = customer_ds
#     n1 = nation_ds[(nation_ds["n_name"] == "FRANCE")]
#     n2 = nation_ds[(nation_ds["n_name"] == "GERMANY")]
#
#     # ----- do nation 1 -----
#     N1_C = customer_filtered.merge(
#         n1, left_on="c_nationkey", right_on="n_nationkey", how="inner"
#     )
#     N1_C = N1_C.drop(columns=["c_nationkey", "n_nationkey"]).rename(
#         columns={"n_name": "cust_nation"}
#     )
#     N1_C_O = N1_C.merge(
#         orders_filtered, left_on="c_custkey", right_on="o_custkey", how="inner"
#     )
#     N1_C_O = N1_C_O.drop(columns=["c_custkey", "o_custkey"])
#
#     N2_S = supplier_filtered.merge(
#         n2, left_on="s_nationkey", right_on="n_nationkey", how="inner"
#     )
#     N2_S = N2_S.drop(columns=["s_nationkey", "n_nationkey"]).rename(
#         columns={"n_name": "supp_nation"}
#     )
#     N2_S_L = N2_S.merge(
#         lineitem_filtered, left_on="s_suppkey", right_on="l_suppkey", how="inner"
#     )
#     N2_S_L = N2_S_L.drop(columns=["s_suppkey", "l_suppkey"])
#
#     total1 = N1_C_O.merge(
#         N2_S_L, left_on="o_orderkey", right_on="l_orderkey", how="inner"
#     )
#     total1 = total1.drop(columns=["o_orderkey", "l_orderkey"])
#
#     # ----- do nation 2 ----- (same as nation 1 section but with nation 2)
#     N2_C = customer_filtered.merge(
#         n2, left_on="c_nationkey", right_on="n_nationkey", how="inner"
#     )
#     N2_C = N2_C.drop(columns=["c_nationkey", "n_nationkey"]).rename(
#         columns={"n_name": "cust_nation"}
#     )
#     N2_C_O = N2_C.merge(
#         orders_filtered, left_on="c_custkey", right_on="o_custkey", how="inner"
#     )
#     N2_C_O = N2_C_O.drop(columns=["c_custkey", "o_custkey"])
#
#     N1_S = supplier_filtered.merge(
#         n1, left_on="s_nationkey", right_on="n_nationkey", how="inner"
#     )
#     N1_S = N1_S.drop(columns=["s_nationkey", "n_nationkey"]).rename(
#         columns={"n_name": "supp_nation"}
#     )
#     N1_S_L = N1_S.merge(
#         lineitem_filtered, left_on="s_suppkey", right_on="l_suppkey", how="inner"
#     )
#     N1_S_L = N1_S_L.drop(columns=["s_suppkey", "l_suppkey"])
#
#     total2 = N2_C_O.merge(
#         N1_S_L, left_on="o_orderkey", right_on="l_orderkey", how="inner"
#     )
#     total2 = total2.drop(columns=["o_orderkey", "l_orderkey"])
#
#     # concat results
#     total = dd.concat([total1, total2])
#     result_df = (
#         total.groupby(["supp_nation", "cust_nation", "l_year"])
#         .revenue.agg("sum")
#         .reset_index()
#     )
#     result_df.columns = ["supp_nation", "cust_nation", "l_year", "revenue"]
#
#     result_df.sort_values(
#         by=["supp_nation", "cust_nation", "l_year"],
#         ascending=True,
#     ).compute()
