"""
This data represents a skewed but realistic dataset that dask has been struggling with in the past.
Workflow based on https://github.com/coiled/imbalanced-join/blob/main/test_big_join_synthetic.ipynb
"""
import dask.dataframe as dd
import pytest

LARGE_DF = "s3://test-imbalanced-join/df1/"
SMALL_DF = "s3://test-imbalanced-join/df2/"


@pytest.mark.client("imbalanced_join")
def test_merge(client):
    """Merge large df and small df"""
    large_df = dd.read_parquet(LARGE_DF)
    small_df = dd.read_parquet(SMALL_DF)

    group_cols = ["df2_group", "bucket", "group1", "group2", "group3", "group4"]

    res = large_df.merge(
        right=small_df, how="inner", on=["key", "key"], suffixes=["_l", "_r"]
    )[group_cols + ["value"]]

    res = res.groupby(group_cols, sort=False).agg({"value": "sum"})
    # evaluate the whole merged dataframe
    res["value"].sum().compute()
