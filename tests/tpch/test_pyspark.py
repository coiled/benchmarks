import re
import warnings

import pytest
import requests
from distributed import Event
from urllib3.util import Url, parse_url

pytestmark = pytest.mark.tpch_nondask

pytest.importorskip("coiled.spark")


@pytest.fixture(autouse=True)
def add_pyspark_version(test_run_benchmark):
    if test_run_benchmark:
        import pyspark

        test_run_benchmark.pyspark_version = pyspark.__version__


@pytest.fixture(scope="session")
def cluster_spec(cluster_spec, scale, shutdown_on_close):
    everywhere = dict(
        idle_timeout="2h",
        wait_for_workers=True,
        scheduler_vm_types=["m6i.xlarge"],
        shutdown_on_close=shutdown_on_close,
    )
    if scale == 10000:
        return {
            "worker_vm_types": ["m6i.2xlarge"],
            "account": "dask-engineering",
            "n_workers": 32 * 5,
            "worker_disk_size": 200,
            **everywhere,
        }
    else:
        return cluster_spec


@pytest.fixture()
def client(cluster):
    # Redefine since global client fixture is also already starting the
    # benchmark clock
    with cluster.get_client() as client:
        yield client


@pytest.fixture(autouse=True)
def cheat_idleness(spark_setup, client):
    def wait(ev):
        ev.wait()

    ev = Event()
    fut = client.submit(wait, ev)
    yield
    ev.set()
    fut.result()


@pytest.fixture(scope="module")
def spark_setup(cluster, local):
    pytest.importorskip("pyspark")

    spark_dashboard: Url
    if local:
        from pyspark.sql import SparkSession

        # Set app name to match that used in Coiled Spark
        spark = (
            SparkSession.builder.appName("SparkConnectServer")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            # By default, the driver is only allowed to use up to 1g of memory
            # which causes it to crash when collecting results
            .config("spark.driver.memory", "10g")
            .getOrCreate()
        )
        spark_dashboard = parse_url("http://localhost:4040")
    else:
        spark = cluster.get_spark(executor_memory_factor=0.8, worker_memory_factor=0.9)
        # Available on coiled>=1.12.4
        if not hasattr(cluster, "_spark_dashboard"):
            cluster._spark_dashboard = (
                parse_url(cluster._dashboard_address)._replace(path="/spark").url
            )
        spark_dashboard = parse_url(cluster._spark_dashboard)

    try:
        spark._spark_dashboard: Url = spark_dashboard

        # warm start
        from pyspark.sql import Row

        df = spark.createDataFrame(
            [
                Row(a=1, b=2.0, c="string1"),
            ]
        )
        df.collect()
        yield spark
    finally:
        spark.stop()


def get_number_spark_executors(spark_dashboard: Url):
    base_path = spark_dashboard.path or ""

    url = spark_dashboard._replace(path=f"{base_path}/api/v1/applications")
    apps = requests.get(url.url).json()
    for app in apps:
        if app["name"] == "SparkConnectServer":
            appid = app["id"]
            break
    else:
        raise ValueError("Failed to find Spark application 'SparkConnectServer'")

    url = url._replace(path=f"{url.path}/{appid}/allexecutors")
    executors = requests.get(url.url).json()
    return sum(1 for executor in executors if executor["isActive"])


@pytest.fixture
def spark(request, spark_setup, benchmark_all, client):
    n_executors_start = get_number_spark_executors(spark_setup._spark_dashboard)
    with benchmark_all(client):
        yield spark_setup
    n_executors_finish = get_number_spark_executors(spark_setup._spark_dashboard)

    if n_executors_finish != n_executors_start:
        msg = (
            "Executor count changed between start and end of yield. "
            f"Startd with {n_executors_start}, ended with {n_executors_finish}"
        )
        if request.config.getoption("ignore-spark-executor-count"):
            warnings.warn(msg)
        else:
            raise RuntimeError(msg)

    spark_setup.catalog.clearCache()


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
    spark.sql(query).collect()


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

    spark.sql(query).collect()


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

    spark.sql(query).collect()


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

    spark.sql(query).collect()


@pytest.mark.skip()
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
    spark.sql(query).collect()


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

    spark.sql(query).collect()


@pytest.mark.skip()
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

    spark.sql(query).collect()


def test_query_8(spark, dataset_path):
    for name in (
        "part",
        "supplier",
        "lineitem",
        "orders",
        "customer",
        "nation",
        "region",
    ):
        register_table(spark, dataset_path, name)

    query = """
    select
        o_year,
        round(
            sum(case
                when nation = 'BRAZIL' then volume
                else 0
            end) / sum(volume)
        , 2) as mkt_share
    from
        (
            select
                extract(year from o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) as volume,
                n2.n_name as nation
            from
                part,
                supplier,
                lineitem,
                orders,
                customer,
                nation n1,
                nation n2,
                region
            where
                p_partkey = l_partkey
                and s_suppkey = l_suppkey
                and l_orderkey = o_orderkey
                and o_custkey = c_custkey
                and c_nationkey = n1.n_nationkey
                and n1.n_regionkey = r_regionkey
                and r_name = 'AMERICA'
                and s_nationkey = n2.n_nationkey
                and o_orderdate between date '1995-01-01' and date '1996-12-31'
                and p_type = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
    group by
        o_year
    order by
        o_year
    """

    spark.sql(query).collect()


def test_query_9(spark, dataset_path):
    for name in ("part", "supplier", "lineitem", "partsupp", "orders", "nation"):
        register_table(spark, dataset_path, name)

    query = """
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

    spark.sql(query).collect()


def test_query_10(spark, dataset_path):
    for name in ("customer", "orders", "lineitem", "nation"):
        register_table(spark, dataset_path, name)

    query = """
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

    spark.sql(query).collect()


def test_query_11(spark, dataset_path, scale):
    for name in ("partsupp", "supplier", "nation"):
        register_table(spark, dataset_path, name)

    query = f"""
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
        ps_partkey having
                sum(ps_supplycost * ps_availqty) > (
            select
                sum(ps_supplycost * ps_availqty) * {0.0001 / scale}
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

    spark.sql(query).collect()


def test_query_12(spark, dataset_path):
    for name in ("orders", "lineitem"):
        register_table(spark, dataset_path, name)

    query = """
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

    spark.sql(query).collect()


def test_query_13(spark, dataset_path):
    for name in ("customer", "orders"):
        register_table(spark, dataset_path, name)

    query = """
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

    spark.sql(query).collect()


def test_query_14(spark, dataset_path):
    for name in ("lineitem", "part"):
        register_table(spark, dataset_path, name)

    query = """
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

    spark.sql(query).collect()


def test_query_15(spark, dataset_path):
    for name in ("lineitem", "supplier"):
        register_table(spark, dataset_path, name)

    ddl = """
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
    """
    spark.sql(ddl)

    query = """
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

    spark.sql(query).collect()
    spark.sql("drop view revenue")


def test_query_16(spark, dataset_path):
    for name in ("partsupp", "part", "supplier"):
        register_table(spark, dataset_path, name)

    query = """
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

    spark.sql(query).collect()


def test_query_17(spark, dataset_path):
    for name in ("lineitem", "part"):
        register_table(spark, dataset_path, name)

    query = """
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

    spark.sql(query).collect()


@pytest.mark.skip()
def test_query_18(spark, dataset_path):
    for name in ("customer", "orders", "lineitem"):
        register_table(spark, dataset_path, name)

    query = """
    select
        c_name,
        c_custkey,
        o_orderkey,
        to_date(o_orderdate) as o_orderdat,
        o_totalprice,
        DOUBLE(sum(l_quantity)) as col6
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

    spark.sql(query).collect()


def test_query_19(spark, dataset_path):
    for name in ("lineitem", "part"):
        register_table(spark, dataset_path, name)

    query = """
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

    spark.sql(query).collect()


def test_query_20(spark, dataset_path):
    for name in ("supplier", "nation", "partsupp", "part", "lineitem"):
        register_table(spark, dataset_path, name)

    query = """
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

    spark.sql(query).collect()


def test_query_21(spark, dataset_path):
    for name in ("supplier", "lineitem", "orders", "nation"):
        register_table(spark, dataset_path, name)

    query = """
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

    spark.sql(query).collect()


def test_query_22(spark, dataset_path):
    for name in ("customer", "orders"):
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

    spark.sql(query).collect()


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
