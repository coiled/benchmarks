from tests.benchmarks.test_tpch_pyspark import DATASETS, ENABLED_DATASET

ddl = f"""
create table customer as select * from read_parquet('{DATASETS[ENABLED_DATASET]}customer/*.parquet');
create table orders as select * from read_parquet('{DATASETS[ENABLED_DATASET]}orders/*.parquet');
create table lineitem as select * from read_parquet('{DATASETS[ENABLED_DATASET]}lineitem/*.parquet');
"""

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
