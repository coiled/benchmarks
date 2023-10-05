ddl = """
create temp view revenue (supplier_no, total_revenue) as
        select
            l_suppkey,
            sum(l_extendedprice * (1 - l_discount))
        from
            lineitem
        where
            cast(from_unixtime(l_shipdate) as date) >= date '1996-01-01'
            and cast(from_unixtime(l_shipdate) as date) < date '1996-01-01' + interval '3' month
        group by
            l_suppkey
    """

query = """
    select
        s_suppkey,
        s_name,
        s_address,
        s_phone,
        total_revenue
    from
        supplier,
        revenue
    where
        s_suppkey = supplier_no
        and total_revenue = (
            select
                max(total_revenue)
            from
                revenue
        )
    order by
        s_suppkey
"""


def setup(spark):
    from .utils import read_parquet_spark

    for name in ("lineitem", "supplier"):
        read_parquet_spark(spark, name, name)
