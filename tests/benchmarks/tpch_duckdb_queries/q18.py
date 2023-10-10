from tests.benchmarks.test_tpch_pyspark import DATASETS, ENABLED_DATASET

ddl = f"""
create table customer as select * from read_parquet('{DATASETS[ENABLED_DATASET]}customer/*.parquet');
create table lineitem as select * from read_parquet('{DATASETS[ENABLED_DATASET]}lineitem/*.parquet');
create table orders as select * from read_parquet('{DATASETS[ENABLED_DATASET]}orders/*.parquet');
"""

query = """
    select
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice,
        sum(l_quantity)::float as col6
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
