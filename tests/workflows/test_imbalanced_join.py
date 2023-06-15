"""
This data represents a skewed but realistic dataset that dask has been struggling with in the past.
Workflow based on https://github.com/coiled/imbalanced-join/blob/main/test_big_join_synthetic.ipynb
"""
from functools import partial

import dask.dataframe as dd
import pytest

from ..utils_test import wait


@pytest.mark.client("imbalanced_join")
@pytest.fixture
def dataframes(client):
    large_df = dd.read_parquet("s3://test-imbalanced-join/df1/")
    small_df = dd.read_parquet("s3://test-imbalanced-join/df2/")
    # large dataframe has known divisions, use those
    # to ensure the data is partitioned as expected
    divisions = list(range(1, 40002, 10))
    large_df = large_df.set_index("bucket", drop=False, divisions=divisions)
    wait(large_df, client, 10 * 60)
    wait(small_df, client, 10 * 60)
    yield large_df, small_df


@pytest.mark.client("imbalanced_join")
def test_merge(dataframes, shuffle_method):
    """Merge large df and small df"""
    large_df, small_df = dataframes

    group_cols = ["df2_group", "bucket", "group1", "group2", "group3", "group4"]
    res = large_df.merge(
        right=small_df, how="inner", on=["key", "key"], suffixes=["_l", "_r"]
    )[group_cols + ["value"]]

    # group and aggregate, use split_out so that the final data
    # chunks don't end up aggregating on a single worker
    # TODO: workers are still getting killed, even with split_out
    # (
    #     res.groupby(group_cols, sort=False)
    #     .agg({"value": "sum"}, split_out=4000, shuffle=shuffle_method)
    #     .value.sum()
    #     .compute()
    # )

    def aggregate_partition(part, *, shuffle=None):
        return part.groupby(group_cols, sort=False).agg(
            {"value": "sum"}, shuffle=shuffle, split_out=100
        )

    shuffle_aggregate = partial(aggregate_partition, shuffle=shuffle_method)
    res.map_partitions(shuffle_aggregate).value.sum().compute()
