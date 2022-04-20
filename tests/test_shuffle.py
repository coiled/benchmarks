import dask
import dask.dataframe as dd
import pytest


@pytest.mark.stability
def test_shuffle_simple(small_client, s3_url_factory, s3_storage_options):
    write_url = s3_url_factory("shuffle-test-output")
    test_url = s3_url_factory("shuffle-test-data")

    # 100ms ~12GB
    # 75ms ~15GB
    # 50ms ~23.5GB
    test_df = dask.datasets.timeseries(
        start="2000-01-01", end="2000-12-31", freq="75ms", partition_freq="1D"
    )

    write = test_df.to_parquet(
        test_url,
        overwrite=True,
        compute=False,
        storage_options=s3_storage_options,
    )

    dask.compute(write)

    # Shuffle test starts here
    shuffle_df = dd.read_parquet(test_url, storage_options=s3_storage_options)
    shuffle_df = shuffle_df.shuffle(on="x")
    write = shuffle_df.to_parquet(
        write_url,
        overwrite=True,
        compute=False,
        storage_options=s3_storage_options,
    )

    dask.compute(write)
