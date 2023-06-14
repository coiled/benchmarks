"""
This data represents a skewed but realistic dataset that dask has been struggling with in the past.
Workflow based on https://github.com/coiled/imbalanced-join/blob/main/test_big_join_synthetic.ipynb
"""
import dask.dataframe as dd
import pytest
from dask.distributed import wait

LARGE_DF = "s3://test-imbalanced-join/df1/"
SMALL_DF = "s3://test-imbalanced-join/df2/"


@pytest.fixture(scope="module")
@pytest.mark.client("imbalanced_join")
def large_df(client):
    """Set index on the large dataframe"""
    df = dd.read_parquet(LARGE_DF)
    divisions = list(range(1, 40002, 10))
    df = df.set_index("bucket", drop=False, divisions=divisions)
    df = df.persist()
    wait(df)
    yield df


@pytest.mark.client("imbalanced_join")
def test_merge(client, large_df):
    """Merge large df and small df"""
    group_cols = ["df2_group", "bucket", "group1", "group2", "group3", "group4"]
    small_df = dd.read_parquet(SMALL_DF)

    output = large_df.merge(
        right=small_df, how="inner", on=["key", "key"], suffixes=["_l", "_r"]
    )[group_cols + ["value"]]

    def aggregate_partition(part):
        return part.groupby(group_cols, sort=False).agg({"value": "sum"})

    output = output.map_partitions(aggregate_partition)
    output["value"].sum().compute()
