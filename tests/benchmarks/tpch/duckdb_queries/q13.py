from tests.benchmarks.tpch.conftest import DATASETS, ENABLED_DATASET

ddl = f"""
create table customer as select * from read_parquet('{DATASETS[ENABLED_DATASET]}customer/*.parquet');
create table orders as select * from read_parquet('{DATASETS[ENABLED_DATASET]}orders/*.parquet');
"""

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
