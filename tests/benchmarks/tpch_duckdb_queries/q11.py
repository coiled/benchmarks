from tests.benchmarks.test_tpch_pyspark import DATASETS, ENABLED_DATASET

ddl = f"""
create table partsupp as select * from read_parquet('{DATASETS[ENABLED_DATASET]}partsupp/*.parquet');
create table supplier as select * from read_parquet('{DATASETS[ENABLED_DATASET]}supplier/*.parquet');
create table nation as select * from read_parquet('{DATASETS[ENABLED_DATASET]}nation/*.parquet');
"""


query = """
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
