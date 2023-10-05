query = """
select
        round(100.00 * sum(case
            when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
            else 0
        end) / sum(l_extendedprice * (1 - l_discount)), 2) as promo_revenue
    from
        lineitem,
        part
    where
        l_partkey = p_partkey
        and cast(from_unixtime(l_shipdate) as date) >= date '1995-09-01'
        and cast(from_unixtime(l_shipdate) as date) < date '1995-09-01' + interval '1' month
"""


def setup(spark):
    from .utils import read_parquet_spark

    for name in ("lineitem", "part"):
        read_parquet_spark(spark, name, name)
