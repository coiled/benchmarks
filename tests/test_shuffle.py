import dask
import dask.dataframe as dd
import pytest


@pytest.mark.stability
def test_shuffle_simple(small_client):
    df = dask.datasets.timeseries(
        start="2000-01-01", end="2000-12-31", freq="1s", partition_freq="1D"
    )

    sdf = df.shuffle(on="x")

    sdf_size = sdf.memory_usage(index=True).sum() / (1024.0**3)
    assert sdf_size.compute() > 1.0  # 1.0 GB


@pytest.mark.stability
def test_shuffle_parquet(small_client, s3_url_factory, s3_storage_options):
    s3_url = s3_url_factory("shuffle-test-output")
    write_url = s3_url + "/shuffled.parquet"
    test_url = s3_url + "/dataset.parquet"
    # write_url = s3_url_factory("shuffle-test-output")
    # test_url = s3_url_factory("shuffle-test-data")

    # 100ms ~12GB
    # 75ms ~15GB
    # 50ms ~23.5GB
    df = dask.datasets.timeseries(
        start="2000-01-01", end="2000-12-31", freq="50ms", partition_freq="1D"
    )

    write = df.to_parquet(
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
