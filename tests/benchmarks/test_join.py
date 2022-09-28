import dask.dataframe as dd
import pytest

from ..utils_test import cluster_memory, timeseries_of_size


@pytest.mark.parametrize("how", ["outer", "inner"])
def test_join_big(small_client, how):
    memory = cluster_memory(small_client)  # 76.66 GiB

    df1_big = timeseries_of_size(
        memory * 0.9,
    )

    df2_big = timeseries_of_size(
        memory * 0.9,
    )

    dd_result = dd.merge(df1_big, df2_big, how=how).compute()
    assert dd_result


@pytest.mark.parametrize("how", ["outer", "inner"])
def test_join_big_small(small_client, how):
    memory = cluster_memory(small_client)  # 76.66 GiB

    df_big = timeseries_of_size(
        memory * 0.9,
    )

    df_small = timeseries_of_size(
        memory * 0.1,
    )

    dd_result = dd.merge(df_big, df_small, how=how).compute()
    assert dd_result
