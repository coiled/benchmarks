from tests.benchmarks.test_tpch_pyspark import DATASETS, ENABLED_DATASET

ddl = f"""
create table customer as select * from read_parquet('{DATASETS[ENABLED_DATASET]}customer/*.parquet');
create table orders as select * from read_parquet('{DATASETS[ENABLED_DATASET]}orders/*.parquet');
create table lineitem as select * from read_parquet('{DATASETS[ENABLED_DATASET]}lineitem/*.parquet');
create table supplier as select * from read_parquet('{DATASETS[ENABLED_DATASET]}supplier/*.parquet');
create table nation as select * from read_parquet('{DATASETS[ENABLED_DATASET]}nation/*.parquet');
create table region as select * from read_parquet('{DATASETS[ENABLED_DATASET]}region/*.parquet');
"""

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
