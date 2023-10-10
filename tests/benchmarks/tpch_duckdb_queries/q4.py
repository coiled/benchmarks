from tests.benchmarks.test_tpch_pyspark import DATASETS, ENABLED_DATASET

ddl = f"""
create table orders as select * from read_parquet('{DATASETS[ENABLED_DATASET]}orders/*.parquet');
create table lineitem as select * from read_parquet('{DATASETS[ENABLED_DATASET]}lineitem/*.parquet');
"""

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
