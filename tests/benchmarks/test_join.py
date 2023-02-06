import dask
import dask.dataframe as dd
import pytest

from ..utils_test import cluster_memory, run_up_to_nthreads, timeseries_of_size, wait

# (mem_mult, shuffle)
params = [
    (0.1, "tasks"),
    (0.1, "p2p"),
    # (1, "tasks"),
    # (1, "p2p"),
]


@run_up_to_nthreads("small_cluster", 40, reason="Does not finish")
@pytest.mark.parametrize("mem_mult, shuffle", params)
def test_join_big(small_client, mem_mult, shuffle):
    with dask.config.set(shuffle=shuffle):
        memory = cluster_memory(small_client)  # 76.66 GiB

        df1_big = timeseries_of_size(
            memory * mem_mult, dtypes={str(i): float for i in range(100)}
        )  # 66.58 MiB partitions
        df1_big["predicate"] = df1_big["0"] * 1e9
        df1_big = df1_big.astype({"predicate": "int"})

        df2_big = timeseries_of_size(
            memory * mem_mult, dtypes={str(i): float for i in range(100)}
        )  # 66.58 MiB partitions

        # Control cardinality on column to join - this produces cardinality ~ to len(df)
        df2_big["predicate"] = df2_big["0"] * 1e9
        df2_big = df2_big.astype({"predicate": "int"})

        join = dd.merge(df1_big, df2_big, on="predicate", how="inner")
        result = join.size
        wait(result, small_client, 20 * 60)


@pytest.mark.parametrize("mem_mult, shuffle", params)
def test_join_big_small(small_client, mem_mult, shuffle):
    with dask.config.set(shuffle=shuffle):
        memory = cluster_memory(small_client)  # 76.66 GiB

        df_big = timeseries_of_size(
            memory * mem_mult, dtypes={str(i): float for i in range(100)}
        )  # 66.58 MiB partitions

        # Control cardinality on column to join - this produces cardinality ~ to len(df)
        df_big["predicate"] = df_big["0"] * 1e9
        df_big = df_big.astype({"predicate": "int"})

        df_small = timeseries_of_size(
            "100 MB", dtypes={str(i): float for i in range(100)}
        )  # make it obviously small

        df_small["predicate"] = df_small["0"] * 1e9
        df_small_pd = df_small.astype({"predicate": "int"}).compute()

        join = dd.merge(df_big, df_small_pd, on="predicate", how="inner")
        result = join.size
        wait(result, small_client, 20 * 60)
