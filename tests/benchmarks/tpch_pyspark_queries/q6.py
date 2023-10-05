query = """
select
        sum(l_extendedprice * l_discount) as revenue
    from
        lineitem
    where
        cast(from_unixtime(l_shipdate) as date) >= date '1994-01-01'
        and cast(from_unixtime(l_shipdate) as date) < date '1994-01-01' + interval '1' year
        and l_discount between .06 - 0.01 and .06 + 0.01
        and l_quantity < 24
"""


def setup(spark):
    from .utils import read_parquet_spark

    read_parquet_spark(spark, "lineitem", "lineitem")
