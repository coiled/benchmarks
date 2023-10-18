import botocore.session
import pytest

from .conftest import coiled_function

duckdb = pytest.importorskip("duckdb")


@pytest.fixture
def connection(local, restart):
    def _():
        con = duckdb.connect()

        if not local:  # Setup s3 credentials
            session = botocore.session.Session()
            creds = session.get_credentials()
            con.install_extension("httpfs")
            con.load_extension("httpfs")
            con.sql(
                f"""
                SET s3_region='us-east-2';
                SET s3_access_key_id='{creds.access_key}';
                SET s3_secret_access_key='{creds.secret_key}';
                SET s3_session_token='{creds.token}';
                """
            )
        return con

    return _


@coiled_function()
def test_query_1(connection, dataset_path):
    connection().execute(
        f"""
        with lineitem as (
            select * from read_parquet('{dataset_path}lineitem/*.parquet')
        )

        select
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
            l_shipdate <= date '1998-09-02'
        group by
            l_returnflag,
            l_linestatus
        order by
            l_returnflag,
            l_linestatus
        """
    ).arrow()


@coiled_function()
def test_query_2(connection, dataset_path):
    connection().execute(
        f"""
        with part as (select * from read_parquet('{dataset_path}part/*.parquet')),
             supplier as (select * from read_parquet('{dataset_path}supplier/*.parquet')),
             partsupp as (select * from read_parquet('{dataset_path}partsupp/*.parquet')),
             nation as (select * from read_parquet('{dataset_path}nation/*.parquet')),
             region as (select * from read_parquet('{dataset_path}region/*.parquet'))

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
    ).arrow()


@coiled_function()
def test_query_3(connection, dataset_path):
    connection().execute(
        f"""
        with customer as (select * from read_parquet('{dataset_path}customer/*.parquet')),
             orders as (select * from read_parquet('{dataset_path}orders/*.parquet')),
             lineitem as (select * from read_parquet('{dataset_path}lineitem/*.parquet'))

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
    ).arrow()


@coiled_function()
def test_query_4(connection, dataset_path):
    connection().execute(
        f"""
        with orders as (select * from read_parquet('{dataset_path}orders/*.parquet')),
             lineitem as (select * from read_parquet('{dataset_path}lineitem/*.parquet'))

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
    ).arrow()


@coiled_function()
def test_query_5(connection, dataset_path):
    connection().execute(
        f"""
        with customer as (select * from read_parquet('{dataset_path}customer/*.parquet')),
             orders as (select * from read_parquet('{dataset_path}orders/*.parquet')),
             lineitem as (select * from read_parquet('{dataset_path}lineitem/*.parquet')),
             supplier as (select * from read_parquet('{dataset_path}supplier/*.parquet')),
             nation as (select * from read_parquet('{dataset_path}nation/*.parquet')),
             region as (select * from read_parquet('{dataset_path}region/*.parquet'))

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
    ).arrow()


@coiled_function()
def test_query_6(connection, dataset_path):
    connection().execute(
        f"""
        with lineitem as (
            select * from read_parquet('{dataset_path}lineitem/*.parquet')
        )

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
    ).arrow()


@coiled_function()
def test_query_7(connection, dataset_path):
    connection().execute(
        f"""
        with supplier as (select * from read_parquet('{dataset_path}supplier/*.parquet')),
             lineitem as (select * from read_parquet('{dataset_path}lineitem/*.parquet')),
             orders as (select * from read_parquet('{dataset_path}orders/*.parquet')),
             customer as (select * from read_parquet('{dataset_path}customer/*.parquet')),
             nation as (select * from read_parquet('{dataset_path}nation/*.parquet'))

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
    ).arrow()
