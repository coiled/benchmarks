import math

import dask.dataframe as dd
import pandas as pd
import pytest

from ..utils_test import cluster_memory, run_up_to_nthreads, timeseries_of_size, wait


@pytest.fixture(params=[0.1, 1])
def memory_multiplier(request):
    return request.param


@pytest.fixture(params=[10, 50, 100])
def num_cols_freq(request):
    num_cols = request.param
    # Rescale parition frequency such that partition size and number of
    # partitions stay constant when varying columns
    partition_freq = pd.to_timedelta("1d") * num_cols / 100
    return num_cols, partition_freq


@pytest.fixture(
    # percentage of str cols
    params=[
        pytest.param(0, id="no string"),
        pytest.param(50, id="half string"),
        pytest.param(99, id="all string"),
    ]
)
def dtypes(request, num_cols_freq):
    num_cols, _ = num_cols_freq
    num_str_cols = math.floor(request.param * num_cols / 100)
    dtypes: dict[str, type] = {
        str(ix): float for ix in range(0, num_cols - num_str_cols)
    }
    for ix in range(num_cols - num_str_cols, num_cols):
        dtypes[str(ix)] = str
    return dtypes


@run_up_to_nthreads("small_cluster", 40, reason="Does not finish")
def test_join_big(
    small_client, memory_multiplier, configure_shuffling, dtypes, num_cols_freq
):
    _, partition_freq = num_cols_freq
    memory = cluster_memory(small_client)  # 76.66 GiB

    df1_big = timeseries_of_size(
        memory * memory_multiplier,
        dtypes=dtypes,
        partition_freq=partition_freq,
    )  # 66.58 MiB partitions
    df1_big["predicate"] = df1_big["0"] * 1e9
    df1_big = df1_big.astype({"predicate": "int"})

    df2_big = timeseries_of_size(
        memory * memory_multiplier,
        dtypes=dtypes,
        partition_freq=partition_freq,
    )  # 66.58 MiB partitions

    # Control cardinality on column to join - this produces cardinality ~ to len(df)
    df2_big["predicate"] = df2_big["0"] * 1e9
    df2_big = df2_big.astype({"predicate": "int"})

    join = dd.merge(df1_big, df2_big, on="predicate", how="inner")
    result = join.size
    wait(result, small_client, 20 * 60)


def test_join_big_small(
    small_client, memory_multiplier, configure_shuffling, dtypes, num_cols_freq
):
    _, partition_freq = num_cols_freq
    memory = cluster_memory(small_client)  # 76.66 GiB

    df_big = timeseries_of_size(
        memory * memory_multiplier,
        dtypes=dtypes,
        partition_freq=partition_freq,
    )  # 66.58 MiB partitions

    # Control cardinality on column to join - this produces cardinality ~ to len(df)
    df_big["predicate"] = df_big["0"] * 1e9
    df_big = df_big.astype({"predicate": "int"})

    df_small = timeseries_of_size(
        "100 MB",
        dtypes=dtypes,
        partition_freq=partition_freq,
    )  # make it obviously small

    df_small["predicate"] = df_small["0"] * 1e9
    df_small_pd = df_small.astype({"predicate": "int"}).compute()

    join = dd.merge(df_big, df_small_pd, on="predicate", how="inner")
    result = join.size
    wait(result, small_client, 20 * 60)


@pytest.mark.parametrize("persist", [True, False])
def test_set_index(
    small_client, persist, memory_multiplier, configure_shuffling, dtypes, num_cols_freq
):
    _, partition_freq = num_cols_freq
    memory = cluster_memory(small_client)  # 76.66 GiB

    df_big = timeseries_of_size(
        memory * memory_multiplier,
        dtypes=dtypes,
        partition_freq=partition_freq,
    )  # 66.58 MiB partitions
    df_big["predicate"] = df_big["0"] * 1e9
    df_big = df_big.astype({"predicate": "int"})
    if persist:
        df_big = df_big.persist()
    df_indexed = df_big.set_index("0")
    wait(df_indexed.size, small_client, 20 * 60)
