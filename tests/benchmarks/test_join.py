import pytest

from ..utils_test import cluster_memory, run_up_to_nthreads, timeseries_of_size, wait


@run_up_to_nthreads("small_cluster", 40, reason="Does not finish")
def test_join_big(small_client, memory_multiplier, configure_shuffling):
    memory = cluster_memory(small_client)  # 76.66 GiB

    df1_big = timeseries_of_size(
        memory * memory_multiplier, dtypes={str(i): float for i in range(100)}
    )  # 66.58 MiB partitions
    df1_big["predicate"] = df1_big["0"] * 1e9
    df1_big = df1_big.astype({"predicate": "int"})

    df2_big = timeseries_of_size(
        memory * memory_multiplier, dtypes={str(i): float for i in range(100)}
    )  # 66.58 MiB partitions

    # Control cardinality on column to join - this produces cardinality ~ to len(df)
    df2_big["predicate"] = df2_big["0"] * 1e9
    df2_big = df2_big.astype({"predicate": "int"})

    join = df1_big.merge(df2_big, on="predicate", how="inner")
    result = join.size
    wait(result, small_client, 20 * 60)


def test_join_big_small(small_client, memory_multiplier, configure_shuffling):
    if memory_multiplier == 0.1:
        raise pytest.skip(reason="Too noisy; not adding anything to multiplier=1")

    memory = cluster_memory(small_client)  # 76.66 GiB

    df_big = timeseries_of_size(
        memory * memory_multiplier, dtypes={str(i): float for i in range(100)}
    )  # 66.58 MiB partitions

    # Control cardinality on column to join - this produces cardinality ~ to len(df)
    df_big["predicate"] = df_big["0"] * 1e9
    df_big = df_big.astype({"predicate": "int"})

    df_small = timeseries_of_size(
        "100 MB", dtypes={str(i): float for i in range(100)}
    )  # make it obviously small

    df_small["predicate"] = df_small["0"] * 1e9
    df_small_pd = df_small.astype({"predicate": "int"}).compute()

    join = df_big.merge(df_small_pd, on="predicate", how="inner")
    result = join.size
    wait(result, small_client, 20 * 60)


@pytest.mark.parametrize("persist", [True, False])
def test_set_index(small_client, persist, memory_multiplier, configure_shuffling):
    memory = cluster_memory(small_client)  # 76.66 GiB

    df_big = timeseries_of_size(
        memory * memory_multiplier, dtypes={str(i): float for i in range(100)}
    )  # 66.58 MiB partitions
    df_big["predicate"] = df_big["0"] * 1e9
    df_big = df_big.astype({"predicate": "int"})
    if persist:
        df_big = df_big.persist()
    df_indexed = df_big.set_index("0").map_partitions(lambda x: x)
    wait(df_indexed.size, small_client, 20 * 60)
