from tests.benchmarks.tpch.conftest import DATASETS, ENABLED_DATASET

ddl = f"""
create table customer as select * from read_parquet('{DATASETS[ENABLED_DATASET]}customer/*.parquet');
create table orders as select * from read_parquet('{DATASETS[ENABLED_DATASET]}orders/*.parquet');
create table lineitem as select * from read_parquet('{DATASETS[ENABLED_DATASET]}lineitem/*.parquet');
create table nation as select * from read_parquet('{DATASETS[ENABLED_DATASET]}nation/*.parquet');
"""

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
