import dask.dataframe as dd
import pytest

from ..utils_test import cluster_memory, timeseries_of_size

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
def test_join_big(small_client, mem_mult):
    memory = cluster_memory(small_client)  # 76.66 GiB

    df1_big = timeseries_of_size(
        memory * mem_mult,
    )
    df1_big["x2"] = df1_big["x"] * 1e9
    df1_big = df1_big.astype({"x2": "int"})

    df2_big = timeseries_of_size(
        memory * mem_mult,
    )

    df2_big["x2"] = df2_big["x"] * 1e9
    df2_big = df2_big.astype({"x2": "int"})

    dd.merge(df1_big, df2_big, on="x2", how="inner").compute()


@pytest.mark.parametrize("mem_mult", mem_mult)  # [0.1, 1, 10]
def test_join_big_small(small_client, mem_mult):
    memory = cluster_memory(small_client)  # 76.66 GiB

    df_big = timeseries_of_size(
        memory * mem_mult,
    )

    df_big["x2"] = df_big["x"] * 1e9
    df_big = df_big.astype({"x2": "int"})

    df_small = timeseries_of_size(
        memory * 0.01,
    )

    df_small["x2"] = df_small["x"] * 1e9
    df_small = df_small.astype({"x2": "int"})
    df_small = df_small.repartition(npartitions=1)

    dd.merge(df_big, df_small, on="x2", how="inner").compute()
