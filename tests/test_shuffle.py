import uuid

import dask
import dask.dataframe as dd
import pytest


@pytest.fixture(scope="session")
def s3_stability_url(s3, s3_bucket):
    # Ensure that the stability-scratch directory exists,
    # but do NOT reomove it as multiple test runs could be
    # accessing it at the same time
    stability_url = f"{s3_bucket}/stability-scratch"
    s3.mkdirs(stability_url, exist_ok=True)
    return stability_url


@pytest.fixture(scope="session")
def s3_test_data_url(s3, s3_stability_url):
    # Unique, because multiple tests are accessing the bucket
    test_url = f"{s3_stability_url}/test-data-{uuid.uuid4().hex}"

    try:
        s3.makedirs(test_url)
        yield test_url
    finally:
        s3.rm(test_url, recursive=True)


@pytest.fixture
def s3_write_url(s3, s3_stability_url):
    # Unique, because multiple tests are accessing the bucket
    write_url = f"{s3_stability_url}/test-output-{uuid.uuid4().hex}"

    try:
        s3.makedirs(write_url)
        yield write_url
    finally:
        s3.rm(write_url, recursive=True)


@pytest.fixture
def shuffle_dataset(small_client, s3_test_data_url, s3_storage_options):
    """Produces a ~80GB dataset which is about the memory limit of the cluster"""
    df = dask.datasets.timeseries(
        start="2000-01-01", end="2000-12-31", freq="50ms", partition_freq="1D"
    )

    df.to_parquet(
        f"s3://{s3_test_data_url}",
        compute=True,
        overwrite=True,
        storage_options=s3_storage_options,
    )

    yield dd.read_parquet(
        f"s3://{s3_test_data_url}", storage_options=s3_storage_options
    )


@pytest.mark.stability
def test_shuffle_simple(shuffle_dataset, s3_storage_options, s3_write_url):
    sdf = shuffle_dataset.shuffle(on="x")
    write = sdf.to_parquet(
        f"s3://{s3_write_url}",
        compute=False,
        overwrite=True,
        storage_options=s3_storage_options,
    )
    dask.compute(write)
