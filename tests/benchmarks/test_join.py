import dask
import dask.dataframe as dd
import pytest

from ..utils_test import cluster_memory, run_up_to_nthreads, timeseries_of_size

# (mem_mult, shuffle)
params = [
    (0.1, "tasks"),
    # shuffling takes a long time with 1 or higher
    (0.1, "p2p"),
    pytest.param(
        1, "p2p", marks=pytest.mark.skip(reason="client OOMs, see coiled-runtime#633")
    ),
    pytest.param(
        10, "p2p", marks=pytest.mark.skip(reason="client OOMs, see coiled-runtime#633")
    ),
]


@run_up_to_nthreads("small_cluster", 40, reason="Does not finish")
@pytest.mark.parametrize("mem_mult, shuffle", params)
def test_join_big(small_client, mem_mult, shuffle):
    with dask.config.set(shuffle=shuffle):
        memory = cluster_memory(small_client)  # 76.66 GiB

        df1_big = timeseries_of_size(memory * mem_mult)
        df1_big["x2"] = df1_big["x"] * 1e9
        df1_big = df1_big.astype({"x2": "int"})

        df2_big = timeseries_of_size(memory * mem_mult)

        # Control cardinality on column to join - this produces cardinality ~ to len(df)
        df2_big["x2"] = df2_big["x"] * 1e9
        df2_big = df2_big.astype({"x2": "int"})

        dd.merge(df1_big, df2_big, on="x2", how="inner").compute()


@pytest.mark.parametrize("mem_mult, shuffle", params)
def test_join_big_small(small_client, mem_mult, shuffle):
    with dask.config.set(shuffle=shuffle):
        memory = cluster_memory(small_client)  # 76.66 GiB

        df_big = timeseries_of_size(memory * mem_mult)

        # Control cardinality on column to join - this produces cardinality ~ to len(df)
        df_big["x2"] = df_big["x"] * 1e9
        df_big = df_big.astype({"x2": "int"})

        df_small = timeseries_of_size("50 MB")  # make it obviously small

        df_small["x2"] = df_small["x"] * 1e9
        df_small_pd = df_small.astype({"x2": "int"}).compute()

        dd.merge(df_big, df_small_pd, on="x2", how="inner").compute()
