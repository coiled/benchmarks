from datetime import datetime, timedelta

import pandas as pd
import pytest

pytestmark = pytest.mark.tpch_dask

dd = pytest.importorskip("dask_expr")


def test_query_1(client, dataset_path, fs):
    VAR1 = datetime(1998, 9, 2)
    lineitem_ds = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)

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
            "count_order": "size",
        }
    )

    result = total.reset_index().sort_values(["l_returnflag", "l_linestatus"]).compute()

    return result


@pytest.mark.shuffle_p2p
def test_query_2(client, dataset_path, fs):
    var1 = 15
    var2 = "BRASS"
    var3 = "EUROPE"

    region_ds = dd.read_parquet(dataset_path + "region", filesystem=fs)
    nation_filtered = dd.read_parquet(dataset_path + "nation", filesystem=fs)
    supplier_filtered = dd.read_parquet(dataset_path + "supplier", filesystem=fs)
    part_filtered = dd.read_parquet(dataset_path + "part", filesystem=fs)
    partsupp_filtered = dd.read_parquet(dataset_path + "partsupp", filesystem=fs)

    region_filtered = region_ds[(region_ds["r_name"] == var3)]
    r_n_merged = nation_filtered.merge(
        region_filtered, left_on="n_regionkey", right_on="r_regionkey", how="inner"
    )
    s_r_n_merged = r_n_merged.merge(
        supplier_filtered,
        left_on="n_nationkey",
        right_on="s_nationkey",
        how="inner",
    )
    ps_s_r_n_merged = s_r_n_merged.merge(
        partsupp_filtered, left_on="s_suppkey", right_on="ps_suppkey", how="inner"
    )
    part_filtered = part_filtered[
        (part_filtered["p_size"] == var1)
        & (part_filtered["p_type"].astype(str).str.endswith(var2))
    ]
    merged_df = part_filtered.merge(
        ps_s_r_n_merged, left_on="p_partkey", right_on="ps_partkey", how="inner"
    )
    min_values = merged_df.groupby("p_partkey")["ps_supplycost"].min().reset_index()
    min_values.columns = ["P_PARTKEY_CPY", "MIN_SUPPLYCOST"]
    merged_df = merged_df.merge(
        min_values,
        left_on=["p_partkey", "ps_supplycost"],
        right_on=["P_PARTKEY_CPY", "MIN_SUPPLYCOST"],
        how="inner",
    )
    result = (
        merged_df[
            [
                "s_acctbal",
                "s_name",
                "n_name",
                "p_partkey",
                "p_mfgr",
                "s_address",
                "s_phone",
                "s_comment",
            ]
        ]
        .compute()
        .sort_values(
            by=[
                "s_acctbal",
                "n_name",
                "s_name",
                "p_partkey",
            ],
            ascending=[
                False,
                True,
                True,
                True,
            ],
        )
        .head(100)
    )
    return result


@pytest.mark.shuffle_p2p
def test_query_3(client, dataset_path, fs):
    var1 = datetime.strptime("1995-03-15", "%Y-%m-%d")
    var2 = "BUILDING"

    lineitem_ds = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)
    orders_ds = dd.read_parquet(dataset_path + "orders", filesystem=fs)
    cutomer_ds = dd.read_parquet(dataset_path + "customer", filesystem=fs)

    lsel = lineitem_ds.l_shipdate > var1
    osel = orders_ds.o_orderdate < var1
    csel = cutomer_ds.c_mktsegment == var2
    flineitem = lineitem_ds[lsel]
    forders = orders_ds[osel]
    fcustomer = cutomer_ds[csel]
    jn1 = fcustomer.merge(forders, left_on="c_custkey", right_on="o_custkey")
    jn2 = jn1.merge(flineitem, left_on="o_orderkey", right_on="l_orderkey")
    jn2["revenue"] = jn2.l_extendedprice * (1 - jn2.l_discount)
    total = jn2.groupby(["l_orderkey", "o_orderdate", "o_shippriority"])[
        "revenue"
    ].sum()
    result = (
        total.reset_index()
        .sort_values(["revenue"], ascending=False)
        .head(10)[["l_orderkey", "revenue", "o_orderdate", "o_shippriority"]]
    )

    return result


@pytest.mark.shuffle_p2p
def test_query_4(client, dataset_path, fs):
    date1 = datetime.strptime("1993-10-01", "%Y-%m-%d")
    date2 = datetime.strptime("1993-07-01", "%Y-%m-%d")

    line_item_ds = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)
    orders_ds = dd.read_parquet(dataset_path + "orders", filesystem=fs)

    lsel = line_item_ds.l_commitdate < line_item_ds.l_receiptdate
    osel = (orders_ds.o_orderdate < date1) & (orders_ds.o_orderdate >= date2)
    flineitem = line_item_ds[lsel]
    forders = orders_ds[osel]
    jn = forders.merge(
        flineitem, left_on="o_orderkey", right_on="l_orderkey"
    ).drop_duplicates(subset=["o_orderkey"])
    result_df = (
        jn.groupby("o_orderpriority")["o_orderkey"]
        .size()
        .reset_index()
        .sort_values(["o_orderpriority"])
    )
    result = result_df.rename(columns={"o_orderkey": "order_count"}).compute()

    return result


@pytest.mark.shuffle_p2p
def test_query_5(client, dataset_path, fs):
    date1 = datetime.strptime("1994-01-01", "%Y-%m-%d")
    date2 = datetime.strptime("1995-01-01", "%Y-%m-%d")

    region_ds = dd.read_parquet(dataset_path + "region", filesystem=fs)
    nation_ds = dd.read_parquet(dataset_path + "nation", filesystem=fs)
    customer_ds = dd.read_parquet(dataset_path + "customer", filesystem=fs)
    line_item_ds = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)
    orders_ds = dd.read_parquet(dataset_path + "orders", filesystem=fs)
    supplier_ds = dd.read_parquet(dataset_path + "supplier", filesystem=fs)

    rsel = region_ds.r_name == "ASIA"
    osel = (orders_ds.o_orderdate >= date1) & (orders_ds.o_orderdate < date2)
    forders = orders_ds[osel]
    fregion = region_ds[rsel]
    jn1 = fregion.merge(nation_ds, left_on="r_regionkey", right_on="n_regionkey")
    jn2 = jn1.merge(customer_ds, left_on="n_nationkey", right_on="c_nationkey")
    jn3 = jn2.merge(forders, left_on="c_custkey", right_on="o_custkey")
    jn4 = jn3.merge(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
    jn5 = supplier_ds.merge(
        jn4,
        left_on=["s_suppkey", "s_nationkey"],
        right_on=["l_suppkey", "n_nationkey"],
    )
    jn5["revenue"] = jn5.l_extendedprice * (1.0 - jn5.l_discount)
    gb = jn5.groupby("n_name")["revenue"].sum()
    result = gb.reset_index().sort_values("revenue", ascending=False).compute()

    return result


def test_query_6(client, dataset_path, fs):
    date1 = datetime.strptime("1994-01-01", "%Y-%m-%d")
    date2 = datetime.strptime("1995-01-01", "%Y-%m-%d")
    var3 = 24

    line_item_ds = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)

    sel = (
        (line_item_ds.l_shipdate >= date1)
        & (line_item_ds.l_shipdate < date2)
        & (line_item_ds.l_discount >= 0.05)
        & (line_item_ds.l_discount <= 0.07)
        & (line_item_ds.l_quantity < var3)
    )

    flineitem = line_item_ds[sel]
    result = (flineitem.l_extendedprice * flineitem.l_discount).sum().compute()

    return pd.DataFrame({"revenue": [result]})


@pytest.mark.shuffle_p2p
def test_query_7(client, dataset_path, fs):
    var1 = datetime.strptime("1995-01-01", "%Y-%m-%d")
    var2 = datetime.strptime("1997-01-01", "%Y-%m-%d")

    nation_ds = dd.read_parquet(dataset_path + "nation", filesystem=fs)
    customer_ds = dd.read_parquet(dataset_path + "customer", filesystem=fs)
    line_item_ds = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)
    orders_ds = dd.read_parquet(dataset_path + "orders", filesystem=fs)
    supplier_ds = dd.read_parquet(dataset_path + "supplier", filesystem=fs)

    lineitem_filtered = line_item_ds[
        (line_item_ds["l_shipdate"] >= var1) & (line_item_ds["l_shipdate"] < var2)
    ]
    lineitem_filtered["l_year"] = lineitem_filtered["l_shipdate"].dt.year
    lineitem_filtered["revenue"] = lineitem_filtered["l_extendedprice"] * (
        1.0 - lineitem_filtered["l_discount"]
    )

    supplier_filtered = supplier_ds
    orders_filtered = orders_ds
    customer_filtered = customer_ds
    n1 = nation_ds[(nation_ds["n_name"] == "FRANCE")]
    n2 = nation_ds[(nation_ds["n_name"] == "GERMANY")]

    # ----- do nation 1 -----
    N1_C = customer_filtered.merge(
        n1, left_on="c_nationkey", right_on="n_nationkey", how="inner"
    )
    N1_C = N1_C.drop(columns=["c_nationkey", "n_nationkey"]).rename(
        columns={"n_name": "cust_nation"}
    )
    N1_C_O = N1_C.merge(
        orders_filtered, left_on="c_custkey", right_on="o_custkey", how="inner"
    )
    N1_C_O = N1_C_O.drop(columns=["c_custkey", "o_custkey"])

    N2_S = supplier_filtered.merge(
        n2, left_on="s_nationkey", right_on="n_nationkey", how="inner"
    )
    N2_S = N2_S.drop(columns=["s_nationkey", "n_nationkey"]).rename(
        columns={"n_name": "supp_nation"}
    )
    N2_S_L = N2_S.merge(
        lineitem_filtered, left_on="s_suppkey", right_on="l_suppkey", how="inner"
    )
    N2_S_L = N2_S_L.drop(columns=["s_suppkey", "l_suppkey"])

    total1 = N1_C_O.merge(
        N2_S_L, left_on="o_orderkey", right_on="l_orderkey", how="inner"
    )
    total1 = total1.drop(columns=["o_orderkey", "l_orderkey"])

    # ----- do nation 2 ----- (same as nation 1 section but with nation 2)
    N2_C = customer_filtered.merge(
        n2, left_on="c_nationkey", right_on="n_nationkey", how="inner"
    )
    N2_C = N2_C.drop(columns=["c_nationkey", "n_nationkey"]).rename(
        columns={"n_name": "cust_nation"}
    )
    N2_C_O = N2_C.merge(
        orders_filtered, left_on="c_custkey", right_on="o_custkey", how="inner"
    )
    N2_C_O = N2_C_O.drop(columns=["c_custkey", "o_custkey"])

    N1_S = supplier_filtered.merge(
        n1, left_on="s_nationkey", right_on="n_nationkey", how="inner"
    )
    N1_S = N1_S.drop(columns=["s_nationkey", "n_nationkey"]).rename(
        columns={"n_name": "supp_nation"}
    )
    N1_S_L = N1_S.merge(
        lineitem_filtered, left_on="s_suppkey", right_on="l_suppkey", how="inner"
    )
    N1_S_L = N1_S_L.drop(columns=["s_suppkey", "l_suppkey"])

    total2 = N2_C_O.merge(
        N1_S_L, left_on="o_orderkey", right_on="l_orderkey", how="inner"
    )
    total2 = total2.drop(columns=["o_orderkey", "l_orderkey"])

    # concat results
    total = dd.concat([total1, total2])
    result_df = (
        total.groupby(["supp_nation", "cust_nation", "l_year"])
        .revenue.agg("sum")
        .reset_index()
    )
    result_df.columns = ["supp_nation", "cust_nation", "l_year", "revenue"]

    result = result_df.sort_values(
        by=["supp_nation", "cust_nation", "l_year"],
        ascending=True,
    ).compute()

    return result


@pytest.mark.shuffle_p2p
def test_query_9(client, dataset_path, fs):
    """
    select
        nation,
        o_year,
        round(sum(amount), 2) as sum_profit
    from
        (
            select
                n_name as nation,
                year(o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
            from
                part,
                supplier,
                lineitem,
                partsupp,
                orders,
                nation
            where
                s_suppkey = l_suppkey
                and ps_suppkey = l_suppkey
                and ps_partkey = l_partkey
                and p_partkey = l_partkey
                and o_orderkey = l_orderkey
                and s_nationkey = n_nationkey
                and p_name like '%green%'
        ) as profit
    group by
        nation,
        o_year
    order by
        nation,
        o_year desc
    """
    part = dd.read_parquet(dataset_path + "part", filesystem=fs)
    partsupp = dd.read_parquet(dataset_path + "partsupp", filesystem=fs)
    supplier = dd.read_parquet(dataset_path + "supplier", filesystem=fs)
    lineitem = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)
    orders = dd.read_parquet(dataset_path + "orders", filesystem=fs)
    nation = dd.read_parquet(dataset_path + "nation", filesystem=fs)

    subquery = (
        part.merge(partsupp, left_on="p_partkey", right_on="ps_partkey", how="inner")
        .merge(supplier, left_on="ps_suppkey", right_on="s_suppkey", how="inner")
        .merge(
            lineitem,
            left_on=["ps_partkey", "ps_suppkey"],
            right_on=["l_partkey", "l_suppkey"],
            how="inner",
        )
        .merge(orders, left_on="l_orderkey", right_on="o_orderkey", how="inner")
        .merge(nation, left_on="s_nationkey", right_on="n_nationkey", how="inner")
        .assign(part_contains_green=lambda df: df.p_name.str.match("*green*"))
        .query("part_contains_green")
        .rename(columns={"n_name": "nation"})
        .assign(
            o_year=lambda df: df.o_orderdate.dt.year,
            amount=lambda df: df.l_extendedprice * (1 - df.l_discount)
            - df.ps_supplycost * df.l_quantity,
        )
        .loc[:, ["o_year", "nation", "amount"]]
    )

    result = (
        subquery.groupby(["nation", "o_year"])
        .amount.sum()
        .round(2)
        .reset_index()
        .rename(columns={"amount": "sum_profit"})
        .sort_values(by=["nation", "o_year"], ascending=[True, False])
        .compute()
    )

    return result


@pytest.mark.shuffle_p2p
def test_query_10(client, dataset_path, fs):
    """
    select
        c_custkey,
        c_name,
        round(sum(l_extendedprice * (1 - l_discount)), 2) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
    from
        customer,
        orders,
        lineitem,
        nation
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= date '1993-10-01'
        and o_orderdate < date '1993-10-01' + interval '3' month
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
    group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
    order by
        revenue desc
    limit 20
    """
    customer = dd.read_parquet(dataset_path + "customer", filesystem=fs)
    orders = dd.read_parquet(dataset_path + "orders", filesystem=fs)
    lineitem = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)
    nation = dd.read_parquet(dataset_path + "nation", filesystem=fs)

    orderdate_from = datetime.strptime("1993-10-01", "%Y-%m-%d")
    orderdate_to = datetime.strptime("1994-01-01", "%Y-%m-%d")

    query = (
        lineitem.merge(orders, left_on="l_orderkey", right_on="o_orderkey", how="inner")
        .merge(customer, left_on="o_custkey", right_on="c_custkey", how="inner")
        .merge(nation, left_on="c_nationkey", right_on="n_nationkey", how="inner")
    )
    query = query[
        (query.o_orderdate >= orderdate_from)
        & (query.o_orderdate < orderdate_to)
        & (query.l_returnflag == "R")
    ]
    query["revenue"] = query.l_extendedprice * (1 - query.l_discount)
    result = (
        query.groupby(
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
        .revenue.sum()
        .round(2)
        .reset_index()
        .sort_values(by=["revenue"], ascending=[False])
        .head(20)[
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
        ]
    )

    return result


@pytest.mark.shuffle_p2p
def test_query_12(client, dataset_path, fs):
    """
    select
        l_shipmode,
        sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
        sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
    from
        orders,
        lineitem
    where
        o_orderkey = l_orderkey
        and l_shipmode in ('MAIL', 'SHIP')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= date '1994-01-01'
        and l_receiptdate < date '1994-01-01' + interval '1' year
    group by
        l_shipmode
    order by
        l_shipmode
    """
    orders = dd.read_parquet(dataset_path + "orders", filesystem=fs)
    lineitem = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)

    receiptdate_from = datetime.strptime("1994-01-01", "%Y-%m-%d")
    receiptdate_to = receiptdate_from + timedelta(days=365)

    table = orders.merge(
        lineitem, left_on="o_orderkey", right_on="l_orderkey", how="left"
    )
    table = table[
        (table.l_shipmode.isin(("MAIL", "SHIP")))
        & (table.l_commitdate < table.l_receiptdate)
        & (table.l_shipdate < table.l_commitdate)
        & (table.l_receiptdate >= receiptdate_from)
        & (table.l_receiptdate < receiptdate_to)
    ]

    mask = table.o_orderpriority.isin(("1-URGENT", "2-HIGH"))
    table["high_line_count"] = 0
    table["high_line_count"] = table.high_line_count.where(~mask, 1)
    table["low_line_count"] = 0
    table["low_line_count"] = table.low_line_count.where(mask, 1)

    result = (
        table.groupby("l_shipmode")
        .agg({"high_line_count": "sum", "low_line_count": "sum"})
        .reset_index()
        .sort_values(by="l_shipmode")
        .compute()
    )

    return result


@pytest.mark.shuffle_p2p
def test_query_13(client, dataset_path, fs):
    """
    select
        c_count, count(*) as custdist
    from (
        select
            c_custkey,
            count(o_orderkey)
        from
            customer left outer join orders on
            c_custkey = o_custkey
            and o_comment not like '%special%requests%'
        group by
            c_custkey
        )as c_orders (c_custkey, c_count)
    group by
        c_count
    order by
        custdist desc,
        c_count desc
    """
    customer = dd.read_parquet(dataset_path + "customer", filesystem=fs)
    orders = dd.read_parquet(dataset_path + "orders", filesystem=fs)

    subquery = customer.merge(
        orders, left_on="c_custkey", right_on="o_custkey", how="left"
    )
    subquery = subquery[~subquery.o_comment.str.match("*special*requests*")]
    subquery = (
        subquery.groupby("c_custkey")
        .size()
        .to_frame()
        .reset_index()
        .rename(columns={0: "c_count"})[["c_custkey", "c_count"]]
    )

    result = (
        subquery.groupby("c_count")
        .size()
        .to_frame()
        .rename(columns={0: "custdist"})
        .reset_index()
        .sort_values(by=["custdist", "c_count"], ascending=[False, False])
        .compute()
    )

    return result


@pytest.mark.shuffle_p2p
def test_query_14(client, dataset_path, fs):
    """
    select
        round(100.00 * sum(case
            when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
            else 0
        end) / sum(l_extendedprice * (1 - l_discount)), 2) as promo_revenue
    from
        lineitem,
        part
    where
        l_partkey = p_partkey
        and l_shipdate >= date '1995-09-01'
        and l_shipdate < date '1995-09-01' + interval '1' month
    """
    lineitem = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)
    part = dd.read_parquet(dataset_path + "part", filesystem=fs)

    shipdate_from = datetime.strptime("1995-09-01", "%Y-%m-%d")
    shipdate_to = datetime.strptime("1995-10-01", "%Y-%m-%d")

    table = lineitem.merge(part, left_on="l_partkey", right_on="p_partkey", how="inner")
    table = table[
        (table.l_shipdate >= shipdate_from) & (table.l_shipdate < shipdate_to)
    ]

    # Promo revenue by line; CASE clause
    table["promo_revenue"] = table.l_extendedprice * (1 - table.l_discount)
    mask = table.p_type.str.match("PROMO*")
    table["promo_revenue"] = table.promo_revenue.where(mask, 0.00)

    # aggregate promo revenue calculation
    result = (
        (
            100.00
            * table.promo_revenue.sum()
            / (table.l_extendedprice * (1 - table.l_discount)).sum()
        )
        .to_delayed()
        .round(2)
        .compute()
    )

    return pd.DataFrame({"promo_revenue": [result]})


@pytest.mark.shuffle_p2p
def test_query_17(client, dataset_path, fs):
    """
    select
        round(sum(l_extendedprice) / 7.0, 2) as avg_yearly
    from
        lineitem,
        part
    where
        p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container = 'MED BOX'
        and l_quantity < (
            select
                0.2 * avg(l_quantity)
            from
                lineitem
            where
                l_partkey = p_partkey
        )
    """
    lineitem = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)
    part = dd.read_parquet(dataset_path + "part", filesystem=fs)

    avg_qnty_by_partkey = (
        lineitem.groupby("l_partkey")
        .l_quantity.mean()
        .to_frame()
        .rename(columns={"l_quantity": "l_quantity_avg"})
    )

    table = lineitem.merge(
        part, left_on="l_partkey", right_on="p_partkey", how="inner"
    ).merge(avg_qnty_by_partkey, left_on="l_partkey", right_index=True, how="left")

    table = table[
        (table.p_brand == "Brand#23")
        & (table.p_container == "MED BOX")
        & (table.l_quantity < 0.2 * table.l_quantity_avg)
    ]
    result = round((table.l_extendedprice.sum() / 7.0).compute(), 2)

    return pd.DataFrame({"avg_yearly": [result]})


@pytest.mark.shuffle_p2p
def test_query_18(client, dataset_path, fs):
    """
    select
        c_name,
        c_custkey,
        o_orderkey,
        to_date(o_orderdate) as o_orderdat,
        o_totalprice,
        DOUBLE(sum(l_quantity)) as sum
    from
        customer,
        orders,
        lineitem
    where
        o_orderkey in (
            select
                l_orderkey
            from
                lineitem
            group by
                l_orderkey having
                    sum(l_quantity) > 300
        )
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
    group by
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice
    order by
        o_totalprice desc,
        o_orderdate
    limit 100
    """
    customer = dd.read_parquet(dataset_path + "customer", filesystem=fs)
    orders = dd.read_parquet(dataset_path + "orders", filesystem=fs)
    lineitem = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)

    table = customer.merge(
        orders, left_on="c_custkey", right_on="o_custkey", how="inner"
    ).merge(lineitem, left_on="o_orderkey", right_on="l_orderkey", how="inner")

    qnt_over_300 = (
        lineitem.groupby("l_orderkey")
        .l_quantity.sum()
        .to_frame()
        .query("l_quantity > 300")
        .drop(columns=["l_quantity"])
    )

    return (
        table.set_index("l_orderkey")
        .join(qnt_over_300, how="inner")
        .groupby(["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"])
        .l_quantity.sum()
        .reset_index()
        .rename(columns={"l_quantity": "sum"})
        .sort_values(["o_totalprice", "o_orderdate"], ascending=[False, True])
        .compute()  # Can go away after https://github.com/dask-contrib/dask-expr/pull/811
        .head(100)
    )
