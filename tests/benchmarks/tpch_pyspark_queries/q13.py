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


def setup():
    from .utils import read_parquet_spark

    for name in ("customer", "orders"):
        read_parquet_spark(name, name)
