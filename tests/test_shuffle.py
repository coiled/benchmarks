import dask
import dask.dataframe as dd
import pytest


@pytest.mark.stability
def test_shuffle_simple(small_client, s3_url_factory, s3_storage_options):
    test_url = s3_url_factory("shuffle-test-data")
    write_url = s3_url_factory("shuffle-test-output")

    # Generate an 80GB dataset, which is just about the size of the cluster memory limit
    test_df = dask.datasets.timeseries(
        start="2000-01-01", end="2000-12-31", freq="50ms", partition_freq="1D"
    )

    test_df.to_parquet(
        test_url,
        storage_options=s3_storage_options,
    )

    # Shuffle test starts here
    test_df = dd.read_parquet(test_url, storage_options=s3_storage_options)
    shuffle_df = test_df.shuffle(on="x")
    shuffle_df.to_parquet(
        write_url,
        storage_options=s3_storage_options,
    )
