from datetime import datetime, timedelta

import dask_expr as dd


def query_1(dataset_path, fs):
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

    return total.reset_index().sort_values(["l_returnflag", "l_linestatus"])


def query_2(dataset_path, fs):
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
    return (
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
        .head(100, compute=False)
    )


def query_3(dataset_path, fs):
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
    return (
        total.reset_index()
        .sort_values(["revenue"], ascending=False)
        .head(10, compute=False)[
            ["l_orderkey", "revenue", "o_orderdate", "o_shippriority"]
        ]
    )


def query_4(dataset_path, fs):
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
    return result_df.rename(columns={"o_orderkey": "order_count"})


def query_5(dataset_path, fs):
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
    return gb.reset_index().sort_values("revenue", ascending=False)


def query_6(dataset_path, fs):
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
    revenue = (flineitem.l_extendedprice * flineitem.l_discount).to_frame()
    return revenue.sum().to_frame("revenue")


def query_7(dataset_path, fs):
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

    return result_df.sort_values(
        by=["supp_nation", "cust_nation", "l_year"],
        ascending=True,
    )


def query_8(dataset_path, fs):
    var1 = datetime.strptime("1995-01-01", "%Y-%m-%d")
    var2 = datetime.strptime("1997-01-01", "%Y-%m-%d")

    supplier = dd.read_parquet(dataset_path + "supplier", filesystem=fs)
    lineitem = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)
    orders = dd.read_parquet(dataset_path + "orders", filesystem=fs)
    customer = dd.read_parquet(dataset_path + "customer", filesystem=fs)
    nation = dd.read_parquet(dataset_path + "nation", filesystem=fs)
    region = dd.read_parquet(dataset_path + "region", filesystem=fs)
    part = dd.read_parquet(dataset_path + "part", filesystem=fs)

    part = part[part["p_type"] == "ECONOMY ANODIZED STEEL"][["p_partkey"]]
    lineitem["volume"] = lineitem["l_extendedprice"] * (1.0 - lineitem["l_discount"])
    total = part.merge(lineitem, left_on="p_partkey", right_on="l_partkey", how="inner")

    total = total.merge(
        supplier, left_on="l_suppkey", right_on="s_suppkey", how="inner"
    )

    orders = orders[(orders["o_orderdate"] >= var1) & (orders["o_orderdate"] < var2)]
    orders["o_year"] = orders["o_orderdate"].dt.year
    total = total.merge(
        orders, left_on="l_orderkey", right_on="o_orderkey", how="inner"
    )

    total = total.merge(
        customer, left_on="o_custkey", right_on="c_custkey", how="inner"
    )

    n1_filtered = nation[["n_nationkey", "n_regionkey"]]
    total = total.merge(
        n1_filtered, left_on="c_nationkey", right_on="n_nationkey", how="inner"
    )

    n2_filtered = nation[["n_nationkey", "n_name"]].rename(columns={"n_name": "nation"})
    total = total.merge(
        n2_filtered, left_on="s_nationkey", right_on="n_nationkey", how="inner"
    )

    region = region[region["r_name"] == "AMERICA"][["r_regionkey"]]
    total = total.merge(
        region, left_on="n_regionkey", right_on="r_regionkey", how="inner"
    )

    mkt_brazil = (
        total[total["nation"] == "BRAZIL"].groupby("o_year").volume.sum().reset_index()
    )
    mkt_total = total.groupby("o_year").volume.sum().reset_index()

    final = mkt_total.merge(
        mkt_brazil, left_on="o_year", right_on="o_year", suffixes=("_mkt", "_brazil")
    )
    final["mkt_share"] = final.volume_brazil / final.volume_mkt
    return final.sort_values(by=["o_year"], ascending=[True])[["o_year", "mkt_share"]]


def query_9(dataset_path, fs):
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

    part = part[part.p_name.str.contains("green")]

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
    )
    subquery["o_year"] = subquery.o_orderdate.dt.year
    subquery["nation"] = subquery.n_name
    subquery["amount"] = (
        subquery.l_extendedprice * (1 - subquery.l_discount)
        - subquery.ps_supplycost * subquery.l_quantity
    )
    subquery = subquery[["o_year", "nation", "amount"]]

    return (
        subquery.groupby(["nation", "o_year"])
        .amount.sum()
        .round(2)
        .reset_index()
        .rename(columns={"amount": "sum_profit"})
        .sort_values(by=["nation", "o_year"], ascending=[True, False])
    )


def query_10(dataset_path, fs):
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

    orders = orders[
        (orders.o_orderdate >= orderdate_from) & (orders.o_orderdate < orderdate_to)
    ]
    lineitem = lineitem[lineitem.l_returnflag == "R"]

    query = (
        lineitem.merge(orders, left_on="l_orderkey", right_on="o_orderkey", how="inner")
        .merge(customer, left_on="o_custkey", right_on="c_custkey", how="inner")
        .merge(nation, left_on="c_nationkey", right_on="n_nationkey", how="inner")
    )

    # TODO: ideally the filters are pushed up before the merge during optimization
    # query = query[
    #     (query.o_orderdate >= orderdate_from)
    #     & (query.o_orderdate < orderdate_to)
    #     & (query.l_returnflag == "R")
    # ]

    query["revenue"] = query.l_extendedprice * (1 - query.l_discount)
    return (
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
        .head(20, compute=False)[
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


def query_11(dataset_path, fs):
    """
    select
        ps_partkey,
        round(sum(ps_supplycost * ps_availqty), 2) as value
    from
        partsupp,
        supplier,
        nation
    where
        ps_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'GERMANY'
    group by
        ps_partkey
    having
            sum(ps_supplycost * ps_availqty) > (
        select
            sum(ps_supplycost * ps_availqty) * 0.0001
        from
            partsupp,
            supplier,
            nation
        where
            ps_suppkey = s_suppkey
            and s_nationkey = n_nationkey
            and n_name = 'GERMANY'
        )
    order by
        value desc
    """
    partsupp = dd.read_parquet(dataset_path + "partsupp", filesystem=fs)
    supplier = dd.read_parquet(dataset_path + "supplier", filesystem=fs)
    nation = dd.read_parquet(dataset_path + "nation", filesystem=fs)

    joined = partsupp.merge(
        supplier, left_on="ps_suppkey", right_on="s_suppkey", how="inner"
    ).merge(nation, left_on="s_nationkey", right_on="n_nationkey", how="inner")
    joined = joined[joined.n_name == "GERMANY"]

    threshold = (joined.ps_supplycost * joined.ps_availqty).sum() * 0.0001

    joined["value"] = joined.ps_supplycost * joined.ps_availqty

    res = joined.groupby("ps_partkey")["value"].sum()
    res = (
        res[res > threshold]
        .round(2)
        .reset_index()
        .sort_values(by="value", ascending=False)
    )

    return res


def query_12(dataset_path, fs):
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

    return (
        table.groupby("l_shipmode")
        .agg({"high_line_count": "sum", "low_line_count": "sum"})
        .reset_index()
        .sort_values(by="l_shipmode")
    )


def query_13(dataset_path, fs):
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
    orders = orders[~orders.o_comment.str.contains("special.*requests")]
    subquery = customer.merge(
        orders, left_on="c_custkey", right_on="o_custkey", how="left"
    )
    subquery = (
        subquery.groupby("c_custkey")
        .o_orderkey.count()
        .to_frame()
        .reset_index()
        .rename(columns={"o_orderkey": "c_count"})[["c_custkey", "c_count"]]
    )
    return (
        subquery.groupby("c_count")
        .size()
        .to_frame()
        .rename(columns={0: "custdist"})
        .reset_index()
        .sort_values(by=["custdist", "c_count"], ascending=[False, False])
    )


def query_14(dataset_path, fs):
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

    total_promo_revenue = table.promo_revenue.to_frame().sum().reset_index(drop=True)
    total_revenue = (
        (table.l_extendedprice * (1 - table.l_discount))
        .to_frame()
        .sum()
        .reset_index(drop=True)
    )
    # aggregate promo revenue calculation
    return (
        (100.00 * total_promo_revenue / total_revenue)
        .round(2)
        .to_frame("promo_revenue")
    )


def query_15(dataset_path, fs):
    """
    DDL:
    create temp view revenue (supplier_no, total_revenue) as
        select
            l_suppkey,
            sum(l_extendedprice * (1 - l_discount))
        from
            lineitem
        where
            l_shipdate >= date '1996-01-01'
            and l_shipdate < date '1996-01-01' + interval '3' month
        group by
            l_suppkey

    QUERY:
    select
        s_suppkey,
        s_name,
        s_address,
        s_phone,
        total_revenue
    from
        supplier,
        revenue
    where
        s_suppkey = supplier_no
        and total_revenue = (
            select
                max(total_revenue)
            from
                revenue
        )
    order by
        s_suppkey
    """
    lineitem = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)
    supplier = dd.read_parquet(dataset_path + "supplier", filesystem=fs)

    shipdate_from = datetime.strptime("1996-01-01", "%Y-%m-%d")
    shipdate_to = datetime.strptime("1996-04-01", "%Y-%m-%d")

    # Create revenue view
    lineitem = lineitem[
        (lineitem.l_shipdate >= shipdate_from) & (lineitem.l_shipdate < shipdate_to)
    ]
    lineitem["revenue"] = lineitem.l_extendedprice * (1 - lineitem.l_discount)
    revenue = (
        lineitem.groupby("l_suppkey")
        .revenue.sum()
        .to_frame()
        .reset_index()
        .rename(columns={"revenue": "total_revenue", "l_suppkey": "supplier_no"})
    )

    # Query
    table = supplier.merge(
        revenue, left_on="s_suppkey", right_on="supplier_no", how="inner"
    )
    return table[table.total_revenue == revenue.total_revenue.max()][
        ["s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"]
    ].sort_values(by="s_suppkey")


def query_16(dataset_path, fs):
    """
    select
        p_brand,
        p_type,
        p_size,
        count(distinct ps_suppkey) as supplier_cnt
    from
        partsupp,
        part
    where
        p_partkey = ps_partkey
        and p_brand <> 'Brand#45'
        and p_type not like 'MEDIUM POLISHED%'
        and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
        and ps_suppkey not in (
            select
                s_suppkey
            from
                supplier
            where
                s_comment like '%Customer%Complaints%'
        )
    group by
        p_brand,
        p_type,
        p_size
    order by
        supplier_cnt desc,
        p_brand,
        p_type,
        p_size
    """
    partsupp = dd.read_parquet(dataset_path + "partsupp", filesystem=fs)
    part = dd.read_parquet(dataset_path + "part", filesystem=fs)
    supplier = dd.read_parquet(dataset_path + "supplier", filesystem=fs)

    supplier["is_complaint"] = supplier.s_comment.str.contains("Customer.*Complaints")
    # FIXME: We have to compute this early because passing a `dask_expr.Series` to `isin` is not supported
    complaint_suppkeys = supplier[supplier.is_complaint].s_suppkey.compute()

    table = partsupp.merge(part, left_on="ps_partkey", right_on="p_partkey")
    table = table[
        (table.p_brand != "Brand#45")
        & (~table.p_type.str.startswith("MEDIUM POLISHED"))
        & (table.p_size.isin((49, 14, 23, 45, 19, 3, 36, 9)))
        & (~table.ps_suppkey.isin(complaint_suppkeys))
    ]
    return (
        table.groupby(by=["p_brand", "p_type", "p_size"])
        .ps_suppkey.nunique()
        .reset_index()
        .rename(columns={"ps_suppkey": "supplier_cnt"})
        .sort_values(
            by=["supplier_cnt", "p_brand", "p_type", "p_size"],
            ascending=[False, True, True, True],
        )
    )


def query_17(dataset_path, fs):
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
    return (
        (table.l_extendedprice.to_frame().sum() / 7.0).round(2).to_frame("avg_yearly")
    )


def query_18(dataset_path, fs):
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

    qnt_over_300 = lineitem.groupby("l_orderkey").l_quantity.sum().to_frame()
    qnt_over_300 = qnt_over_300[qnt_over_300.l_quantity > 300].drop(
        columns=["l_quantity"]
    )

    return (
        table.merge(qnt_over_300, on="l_orderkey")
        .groupby(["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"])
        .l_quantity.sum()
        .reset_index()
        .rename(columns={"l_quantity": "sum"})
        .sort_values(["o_totalprice", "o_orderdate"], ascending=[False, True])
        .head(100, compute=False)
    )


def query_19(dataset_path, fs):
    """
    select
        round(sum(l_extendedprice* (1 - l_discount)), 2) as revenue
    from
        lineitem,
        part
    where
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#12'
            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l_quantity >= 1 and l_quantity <= 1 + 10
            and p_size between 1 and 5
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#23'
            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l_quantity >= 10 and l_quantity <= 20
            and p_size between 1 and 10
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
            p_partkey = l_partkey
            and p_brand = 'Brand#34'
            and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l_quantity >= 20 and l_quantity <= 30
            and p_size between 1 and 15
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
    """
    lineitem = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)
    part = dd.read_parquet(dataset_path + "part", filesystem=fs)

    table = lineitem.merge(part, left_on="l_partkey", right_on="p_partkey")
    table = table[
        (
            (table.p_brand == "Brand#12")
            & (table.p_container.isin(("SM CASE", "SM BOX", "SM PACK", "SM PKG")))
            & ((table.l_quantity >= 1) & (table.l_quantity <= 1 + 10))
            & (table.p_size.between(1, 5))
            & (table.l_shipmode.isin(("AIR", "AIR REG")))
            & (table.l_shipinstruct == "DELIVER IN PERSON")
        )
        | (
            (table.p_brand == "Brand#23")
            & (table.p_container.isin(("MED BAG", "MED BOX", "MED PKG", "MED PACK")))
            & ((table.l_quantity >= 10) & (table.l_quantity <= 20))
            & (table.p_size.between(1, 10))
            & (table.l_shipmode.isin(("AIR", "AIR REG")))
            & (table.l_shipinstruct == "DELIVER IN PERSON")
        )
        | (
            (table.p_brand == "Brand#34")
            & (table.p_container.isin(("LG CASE", "LG BOX", "LG PACK", "LG PKG")))
            & ((table.l_quantity >= 20) & (table.l_quantity <= 30))
            & (table.p_size.between(1, 15))
            & (table.l_shipmode.isin(("AIR", "AIR REG")))
            & (table.l_shipinstruct == "DELIVER IN PERSON")
        )
    ]
    return (
        (table.l_extendedprice * (1 - table.l_discount))
        .to_frame()
        .sum()
        .round(2)
        .to_frame("revenue")
    )


def query_20(dataset_path, fs):
    """
    select
        s_name,
        s_address
    from
        supplier,
        nation
    where
        s_suppkey in (
            select
                ps_suppkey
            from
                partsupp
            where
                ps_partkey in (
                    select
                        p_partkey
                    from
                        part
                    where
                        p_name like 'forest%'
                )
                and ps_availqty > (
                    select
                        0.5 * sum(l_quantity)
                    from
                        lineitem
                    where
                        l_partkey = ps_partkey
                        and l_suppkey = ps_suppkey
                        and l_shipdate >= date '1994-01-01'
                        and l_shipdate < date '1994-01-01' + interval '1' year
                )
        )
        and s_nationkey = n_nationkey
        and n_name = 'CANADA'
    order by
        s_name
    """
    lineitem = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)
    supplier = dd.read_parquet(dataset_path + "supplier", filesystem=fs)
    nation = dd.read_parquet(dataset_path + "nation", filesystem=fs)
    part = dd.read_parquet(dataset_path + "part", filesystem=fs)
    partsupp = dd.read_parquet(dataset_path + "partsupp", filesystem=fs)

    shipdate_from = datetime.strptime("1994-01-01", "%Y-%m-%d")
    shipdate_to = datetime.strptime("1995-01-01", "%Y-%m-%d")

    res_1 = lineitem[
        (lineitem["l_shipdate"] >= shipdate_from)
        & (lineitem["l_shipdate"] < shipdate_to)
    ]
    res_1 = (
        res_1.groupby(["l_partkey", "l_suppkey"])["l_quantity"]
        .sum()
        .rename("sum_quantity")
        .reset_index()
    )
    res_1["sum_quantity"] = res_1["sum_quantity"] * 0.5
    res_2 = nation[nation["n_name"] == "CANADA"]
    res_3 = supplier.merge(res_2, left_on="s_nationkey", right_on="n_nationkey")

    q_final = (
        part[part["p_name"].str.strip().str.startswith("forest")][["p_partkey"]]
        .drop_duplicates()
        .merge(partsupp, left_on="p_partkey", right_on="ps_partkey")
        .merge(
            res_1,
            left_on=["ps_suppkey", "p_partkey"],
            right_on=["l_suppkey", "l_partkey"],
        )
    )
    q_final = q_final[q_final["ps_availqty"] > q_final["sum_quantity"]][
        ["ps_suppkey"]
    ].drop_duplicates()
    q_final = q_final.merge(res_3, left_on="ps_suppkey", right_on="s_suppkey")
    q_final["s_address"] = q_final["s_address"].str.strip()
    return q_final[["s_name", "s_address"]].sort_values("s_name", ascending=True)


def query_21(dataset_path, fs):
    """
    select
        s_name,
        count(*) as numwait
    from
        supplier,
        lineitem l1,
        orders,
        nation
    where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
            select
                *
            from
                lineitem l2
            where
                l2.l_orderkey = l1.l_orderkey
                and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
            select
                *
            from
                lineitem l3
            where
                l3.l_orderkey = l1.l_orderkey
                and l3.l_suppkey <> l1.l_suppkey
                and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'SAUDI ARABIA'
    group by
        s_name
    order by
        numwait desc,
        s_name
    limit 100
    """
    supplier = dd.read_parquet(dataset_path + "supplier", filesystem=fs)
    lineitem = dd.read_parquet(dataset_path + "lineitem", filesystem=fs)
    orders = dd.read_parquet(dataset_path + "orders", filesystem=fs)
    nation = dd.read_parquet(dataset_path + "nation", filesystem=fs)

    NATION = "SAUDI ARABIA"

    res_1 = (
        lineitem.groupby("l_orderkey")["l_suppkey"]
        .nunique()
        .rename("nunique_col")
        .reset_index()
    )
    res_1 = res_1[res_1["nunique_col"] > 1].merge(
        lineitem[lineitem["l_receiptdate"] > lineitem["l_commitdate"]], on="l_orderkey"
    )

    q_final = (
        res_1.groupby("l_orderkey")["l_suppkey"]
        .nunique()
        .rename("nunique_col")
        .reset_index()
        .merge(res_1, on="l_orderkey", suffixes=("", "_right"))
        .merge(supplier, left_on="l_suppkey", right_on="s_suppkey")
        .merge(nation, left_on="s_nationkey", right_on="n_nationkey")
        .merge(orders, left_on="l_orderkey", right_on="o_orderkey")
    )

    predicate = (
        (q_final["nunique_col"] == 1)
        & (q_final["n_name"] == NATION)
        & (q_final["o_orderstatus"] == "F")
    )

    return (
        q_final[predicate]
        .groupby("s_name")
        .size()
        .rename("numwait")
        .reset_index()
        .sort_values(["numwait", "s_name"], ascending=[False, True])
        .head(100, compute=False)
    )


def query_22(dataset_path, fs):
    """
    select
        cntrycode,
        count(*) as numcust,
        sum(c_acctbal) as totacctbal
    from (
        select
            substring(c_phone from 1 for 2) as cntrycode,
            c_acctbal
        from
            customer
        where
            substring(c_phone from 1 for 2) in
                (13, 31, 23, 29, 30, 18, 17)
            and c_acctbal > (
                select
                    avg(c_acctbal)
                from
                    customer
                where
                    c_acctbal > 0.00
                    and substring (c_phone from 1 for 2) in
                        (13, 31, 23, 29, 30, 18, 17)
            )
            and not exists (
                select
                    *
                from
                    orders
                where
                    o_custkey = c_custkey
            )
        ) as custsale
    group by
        cntrycode
    order by
        cntrycode
    """
    orders_ds = dd.read_parquet(dataset_path + "orders", filesystem=fs)
    customer_ds = dd.read_parquet(dataset_path + "customer", filesystem=fs)

    customers = customer_ds
    customers["cntrycode"] = customers["c_phone"].str.strip().str.slice(0, 2)
    customers = customers[
        customers["cntrycode"].isin(("13", "31", "23", "29", "30", "18", "17"))
    ]

    average_c_acctbal = customers[customers["c_acctbal"] > 0.0]["c_acctbal"].mean()

    custsale = customers[customers["c_acctbal"] > average_c_acctbal]
    custsale = custsale.merge(
        orders_ds, left_on="c_custkey", right_on="o_custkey", how="left"
    )
    custsale = custsale[custsale["o_custkey"].isnull()]
    custsale = custsale.groupby("cntrycode").agg({"c_acctbal": ["size", "sum"]})
    custsale.columns = custsale.columns.get_level_values(-1)
    return (
        custsale.rename(columns={"sum": "totacctbal", "size": "numcust"})
        .reset_index()
        .sort_values("cntrycode", ascending=True)
    )
