import dask
import dask.dataframe as dd
import pytest


@pytest.fixture(params=["tasks", "p2p"])
def shuffle(request):
    return request.param


@pytest.mark.stability
def test_shuffle_simple(small_client, shuffle):
    df = dask.datasets.timeseries(
        start="2000-01-01", end="2000-12-31", freq="1s", partition_freq="1D"
    )

    sdf = df.shuffle(on="x", shuffle=shuffle)

    sdf_size = sdf.memory_usage(index=True).sum() / (1024.0**3)
    assert sdf_size.compute() > 1.0  # 1.0 GB


@pytest.mark.stability
def test_shuffle_parquet(small_client, s3_url, s3_storage_options, shuffle):
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
    df_shuffled = df_shuffled.shuffle(on="x", shuffle=shuffle)
    # Workaround for performance issue. See https://github.com/coiled/coiled-runtime/issues/79
    # for more details. Replace with the line below once using `dask>=2022.04.2`.
    # df_shuffled.to_parquet(shuffled_url, storage_options=s3_storage_options)
    result = df_shuffled.to_parquet(
        shuffled_url, compute=False, storage_options=s3_storage_options
    )
    dask.compute(result)
