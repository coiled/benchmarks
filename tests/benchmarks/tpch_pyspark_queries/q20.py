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
                        and cast(from_unixtime(l_shipdate) as date) >= date '1994-01-01'
                        and cast(from_unixtime(l_shipdate) as date) < date '1994-01-01' + interval '1' year
                )
        )
        and s_nationkey = n_nationkey
        and n_name = 'CANADA'
    order by
        s_name
"""


def setup(spark):
    from .utils import read_parquet_spark

    for name in ("lineitem", "supplier", "nation", "part", "partsupp"):
        read_parquet_spark(spark, name, name)
