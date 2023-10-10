from tests.benchmarks.test_tpch_pyspark import DATASETS, ENABLED_DATASET

ddl = f"""
create table lineitem as select * from read_parquet('{DATASETS[ENABLED_DATASET]}lineitem/*.parquet');
"""

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
