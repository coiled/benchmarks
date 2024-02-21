import dask_expr as dd


def query_11(dataset_path, fs):
    """
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
        ps_partkey
    having
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

    partsupp = dd.read_parquet(dataset_path + "partsupp", filesystem=fs)
    supplier = dd.read_parquet(dataset_path + "supplier", filesystem=fs)
    nation = dd.read_parquet(dataset_path + "nation", filesystem=fs)

    joined = partsupp.merge(
        supplier, left_on="ps_suppkey", right_on="s_suppkey", how="inner"
    ).merge(nation, left_on="s_nationkey", right_on="n_nationkey", how="inner")
    joined = joined[joined.n_name == "GERMANY"]

    threshold = (joined.ps_supplycost * joined.ps_availqty).sum() * 0.0001

    joined["value"] = joined.ps_supplycost * joined.ps_availqty

    # FIXME: https://github.com/dask-contrib/dask-expr/issues/867
    res = joined.groupby("ps_partkey")["value"].sum(split_out=True)
    res = (
        res[res > threshold]
        .round(2)
        .reset_index()
        .sort_values(by="value", ascending=False)
    )

    return res
