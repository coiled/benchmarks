from tests.benchmarks.tpch.conftest import DATASETS, ENABLED_DATASET

ddl = f"""
create table supplier as select * from read_parquet('{DATASETS[ENABLED_DATASET]}supplier/*.parquet');
create table nation as select * from read_parquet('{DATASETS[ENABLED_DATASET]}nation/*.parquet');
create table part as select * from read_parquet('{DATASETS[ENABLED_DATASET]}part/*.parquet');
create table partsupp as select * from read_parquet('{DATASETS[ENABLED_DATASET]}partsupp/*.parquet');
create table lineitem as select * from read_parquet('{DATASETS[ENABLED_DATASET]}lineitem/*.parquet');
"""


query = """
    select
        s_name,
        s_address
    from
        supplier,
        nation
    where
        s_suppkey in (
            select
                ps_suppkey
            from
                partsupp
            where
                ps_partkey in (
                    select
                        p_partkey
                    from
                        part
                    where
                        p_name like 'forest%'
                )
                and ps_availqty > (
                    select
                        0.5 * sum(l_quantity)
                    from
                        lineitem
                    where
                        l_partkey = ps_partkey
                        and l_suppkey = ps_suppkey
                        and l_shipdate >= date '1994-01-01'
                        and l_shipdate < date '1994-01-01' + interval '1' year
                )
        )
        and s_nationkey = n_nationkey
        and n_name = 'CANADA'
    order by
        s_name
"""
