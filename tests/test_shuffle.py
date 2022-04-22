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
@pytest.mark.parametrize("compute", [True, False])
def test_shuffle_parquet(small_client, s3_url, s3_storage_options, compute):
    # Write synthetic dataset to S3
    # Notes on how `freq` impacts total dataset size:
    #   - 100ms ~12GB
    #   - 75ms ~15GB
    #   - 50ms ~23.5GB
    dataset_url = s3_url + "/dataset.parquet"
    df = dask.datasets.timeseries(
        start="2000-01-01", end="2000-12-31", freq="50ms", partition_freq="1D"
    )
    df.to_parquet(dataset_url, storage_options=s3_storage_options)

    # Test `read_parquet` + `shuffle` + `to_parquet` works
    shuffled_url = s3_url + "/shuffled.parquet"
    df_shuffled = dd.read_parquet(dataset_url, storage_options=s3_storage_options)
    # df_shuffled = df_shuffled.shuffle(on="x")
    # FIXME: Setting `compute=True` below causes the `to_parquet` call
    # to take much, much longer. This is unexpected and we should fix this upstream.
    if compute:
        df_shuffled.to_parquet(shuffled_url, storage_options=s3_storage_options)
    else:
        result = df_shuffled.to_parquet(
            shuffled_url, compute=False, storage_options=s3_storage_options
        )
        dask.compute(result)
