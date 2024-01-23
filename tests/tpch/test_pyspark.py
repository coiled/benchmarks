import re

import pytest

pytestmark = pytest.mark.tpch_nondask

pytest.importorskip("coiled.spark")


def register_table(spark, path, name):
    path = path.replace("s3://", "s3a://")
    df = spark.read.parquet(path + name)
    df.createOrReplaceTempView(name)


def test_query_1(spark, dataset_path):
    register_table(spark, dataset_path, "lineitem")

    query = """select
            l_returnflag,
            l_linestatus,
            sum(l_quantity) as sum_qty,
            sum(l_extendedprice) as sum_base_price,
            sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
            avg(l_quantity) as avg_qty,
            avg(l_extendedprice) as avg_price,
            avg(l_discount) as avg_disc,
            count(*) as count_order
        from
            lineitem
        where
            l_shipdate <= date('1998-09-02')
        group by
            l_returnflag,
            l_linestatus
        order by
            l_returnflag,
            l_linestatus
    """
    spark.sql(query).show()  # TODO: find better blocking method


def test_query_2(spark, dataset_path):
    for name in ("part", "supplier", "partsupp", "nation", "region"):
        register_table(spark, dataset_path, name)

    query = """
    select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
    from
        part,
        supplier,
        partsupp,
        nation,
        region
    where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = 15
        and p_type like '%BRASS'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
        and ps_supplycost = (
            select
                min(ps_supplycost)
            from
                partsupp,
                supplier,
                nation,
                region
            where
                p_partkey = ps_partkey
                and s_suppkey = ps_suppkey
                and s_nationkey = n_nationkey
                and n_regionkey = r_regionkey
                and r_name = 'EUROPE'
        )
    order by
        s_acctbal desc,
        n_name,
        s_name,
        p_partkey
    limit 100
    """

    spark.sql(query).show()


def test_query_3(spark, dataset_path):
    for name in ("customer", "orders", "lineitem"):
        register_table(spark, dataset_path, name)

    query = """
        select
            l_orderkey,
            sum(l_extendedprice * (1 - l_discount)) as revenue,
            o_orderdate,
            o_shippriority
        from
            customer,
            orders,
            lineitem
        where
            c_mktsegment = 'BUILDING'
            and c_custkey = o_custkey
            and l_orderkey = o_orderkey
            and o_orderdate < date '1995-03-15'
            and l_shipdate > date '1995-03-15'
        group by
            l_orderkey,
            o_orderdate,
            o_shippriority
        order by
            revenue desc,
            o_orderdate
        limit 10
    """

    spark.sql(query).show()


def test_query_4(spark, dataset_path):
    for name in ("orders", "lineitem"):
        register_table(spark, dataset_path, name)

    query = """
        select
            o_orderpriority,
            count(*) as order_count
        from
            orders
        where
            o_orderdate >= date '1993-07-01'
            and o_orderdate < date '1993-07-01' + interval '3' month
            and exists (
                select
                    *
                from
                    lineitem
                where
                    l_orderkey = o_orderkey
                    and l_commitdate < l_receiptdate
            )
        group by
            o_orderpriority
        order by
            o_orderpriority
    """

    spark.sql(query).show()


def test_query_5(spark, dataset_path):
    for name in (
        "customer",
        "orders",
        "lineitem",
        "supplier",
        "nation",
        "region",
    ):
        register_table(spark, dataset_path, name)

    query = """
        select
            n_name,
            sum(l_extendedprice * (1 - l_discount)) as revenue
        from
            customer,
            orders,
            lineitem,
            supplier,
            nation,
            region
        where
            c_custkey = o_custkey
            and l_orderkey = o_orderkey
            and l_suppkey = s_suppkey
            and c_nationkey = s_nationkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = 'ASIA'
            and o_orderdate >= date '1994-01-01'
            and o_orderdate < date '1994-01-01' + interval '1' year
        group by
            n_name
        order by
            revenue desc
    """
    spark.sql(query).show()


def test_query_6(spark, dataset_path):
    for name in ("lineitem",):
        register_table(spark, dataset_path, name)

    query = """
    select
            sum(l_extendedprice * l_discount) as revenue
        from
            lineitem
        where
            l_shipdate >= date '1994-01-01'
            and l_shipdate < date '1994-01-01' + interval '1' year
            and l_discount between .06 - 0.01 and .06 + 0.01
            and l_quantity < 24
    """

    spark.sql(query).show()


def test_query_7(spark, dataset_path):
    for name in ("supplier", "lineitem", "orders", "customer", "nation"):
        register_table(spark, dataset_path, name)

    query = """
    select
            supp_nation,
            cust_nation,
            l_year,
            sum(volume) as revenue
        from
            (
                select
                    n1.n_name as supp_nation,
                    n2.n_name as cust_nation,
                    year(l_shipdate) as l_year,
                    l_extendedprice * (1 - l_discount) as volume
                from
                    supplier,
                    lineitem,
                    orders,
                    customer,
                    nation n1,
                    nation n2
                where
                    s_suppkey = l_suppkey
                    and o_orderkey = l_orderkey
                    and c_custkey = o_custkey
                    and s_nationkey = n1.n_nationkey
                    and c_nationkey = n2.n_nationkey
                    and (
                        (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                        or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
                    )
                    and l_shipdate between date '1995-01-01' and date '1996-12-31'
            ) as shipping
        group by
            supp_nation,
            cust_nation,
            l_year
        order by
            supp_nation,
            cust_nation,
            l_year
    """

    spark.sql(query).show()


def test_query_22(spark, dataset_path):
    for name in ("orders", "customer"):
        register_table(spark, dataset_path, name)

    query = """
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
                ("13","31","23", "29", "30", "18", "17")
            and c_acctbal > (
                select
                    avg(c_acctbal)
                from
                    customer
                where
                    c_acctbal > 0.00
                    and substring (c_phone from 1 for 2) in
                        ("13","31","23", "29", "30", "18", "17")
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

    spark.sql(query).show()


def fix_timestamp_ns_columns(query):
    """
    scale100 stores l_shipdate/o_orderdate as timestamp[us]
    scale1000 stores l_shipdate/o_orderdate as timestamp[ns] which gives:
        Illegal Parquet type: INT64 (TIMESTAMP(NANOS,true))
    so we set spark.sql.legacy.parquet.nanosAsLong and then convert to timestamp.
    """
    for name in ("l_shipdate", "o_orderdate"):
        query = re.sub(rf"\b{name}\b", f"to_timestamp(cast({name} as string))", query)
    return query
