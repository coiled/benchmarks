import dask
import dask.dataframe as dd
import pytest


@pytest.mark.stability
def test_shuffle_simple(shuffle_dataset, s3_storage_options, s3_url):
    test_url = s3_url("shuffle-test-data")
    write_url = s3_url("shuffle-test-output")

    # An 80GB dataset, which is just about the size of the cluster memory limit
    test_df = dask.datasets.timeseries(
        start="2000-01-01", end="2000-12-31", freq="50ms", partition_freq="1D"
    )

    test_df.to_parquet(
        test_url,
        overwrite=True,
        storage_options=s3_storage_options,
    )

    test_df = dd.read_parquet(test_url, storage_options=s3_storage_options)
    shuffle_df = test_df.shuffle(on="x")

    write = shuffle_df.to_parquet(
        write_url,
        compute=False,
        overwrite=True,
        storage_options=s3_storage_options,
    )

    dask.compute(write)
