from tests.benchmarks.test_tpch_pyspark import DATASETS, ENABLED_DATASET

ddl = f"""
create table supplier as select * from read_parquet('{DATASETS[ENABLED_DATASET]}supplier/*.parquet');
create table lineitem as select * from read_parquet('{DATASETS[ENABLED_DATASET]}lineitem/*.parquet');
create table orders as select * from read_parquet('{DATASETS[ENABLED_DATASET]}orders/*.parquet');
create table nation as select * from read_parquet('{DATASETS[ENABLED_DATASET]}nation/*.parquet');
"""


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
