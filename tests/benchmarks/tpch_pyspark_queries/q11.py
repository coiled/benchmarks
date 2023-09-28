query = """
select
        ps_partkey,
        round(sum(ps_supplycost * ps_availqty), 2) as value
    from
        partsupp,
        supplier,
        nation
    where
        ps_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'GERMANY'
    group by
        ps_partkey having
                sum(ps_supplycost * ps_availqty) > (
            select
                sum(ps_supplycost * ps_availqty) * 0.0001
            from
                partsupp,
                supplier,
                nation
            where
                ps_suppkey = s_suppkey
                and s_nationkey = n_nationkey
                and n_name = 'GERMANY'
            )
        order by
            value desc
"""


def setup():
    from .utils import read_parquet_spark

    for name in ("partsupp", "supplier", "nation"):
        read_parquet_spark(name, name)
