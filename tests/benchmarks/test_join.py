import dask.dataframe as dd
import pytest

from ..utils_test import timeseries_of_size, wait

mem_mult = [
    0.1,
    pytest.param(
        1,
        marks=pytest.mark.skip(reason="Does not finish"),
    ),
    pytest.param(
        10,
        marks=pytest.mark.skip(reason="Does not finish"),
    ),
]


@pytest.mark.parametrize("mem_mult", mem_mult)  # [0.1, 1, 10]
def test_join_big(small_client, cluster_memory, mem_mult):

    df1_big = timeseries_of_size(
        cluster_memory * mem_mult,
    )
    df1_big["x2"] = df1_big["x"] * 1e9
    df1_big = df1_big.astype({"x2": "int"})

    df2_big = timeseries_of_size(
        cluster_memory * mem_mult,
    )

    # Control cardinality on column to join - this produces cardinality ~ to len(df)
    df2_big["x2"] = df2_big["x"] * 1e9
    df2_big = df2_big.astype({"x2": "int"})

    wait(dd.merge(df1_big, df2_big, on="x2", how="inner"), small_client, 240)


@pytest.mark.parametrize("mem_mult", mem_mult)  # [0.1, 1, 10]
def test_join_big_small(small_client, cluster_memory, mem_mult):
    df_big = timeseries_of_size(
        cluster_memory * mem_mult,
    )

    # Control cardinality on column to join - this produces cardinality ~ to len(df)
    df_big["x2"] = df_big["x"] * 1e9
    df_big = df_big.astype({"x2": "int"})

    df_small = timeseries_of_size(
        "50 MB",
    )  # make it obviously small

    df_small["x2"] = df_small["x"] * 1e9
    df_small_pd = df_small.astype({"x2": "int"}).compute()

    wait(dd.merge(df_big, df_small_pd, on="x2", how="inner"), small_client, 60)
