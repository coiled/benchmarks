query = """
    select
        o_orderpriority,
        count(*) as order_count
    from
        orders
    where
        cast(from_unixtime(o_orderdate) as date) >= date '1993-07-01'
        and cast(from_unixtime(o_orderdate) as date) < date '1993-07-01' + interval '3' month
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


def setup(spark):
    from .utils import read_parquet_spark

    for name in ("orders", "lineitem"):
        read_parquet_spark(spark, name, name)
