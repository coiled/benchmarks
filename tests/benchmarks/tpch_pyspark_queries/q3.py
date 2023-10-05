query = """
    select
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        cast(from_unixtime(o_orderdate) as date) as o_orderdate,
        o_shippriority
    from
        customer,
        orders,
        lineitem
    where
        c_mktsegment = 'BUILDING'
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and cast(from_unixtime(o_orderdate) as date) < date '1995-03-15'
        and cast(from_unixtime(l_shipdate) as date) > date '1995-03-15'
    group by
        l_orderkey,
        o_orderdate,
        o_shippriority
    order by
        revenue desc,
        o_orderdate
    limit 10
"""


def setup(spark):
    from .utils import read_parquet_spark

    for name in ("customer", "orders", "lineitem"):
        read_parquet_spark(spark, name, name)
