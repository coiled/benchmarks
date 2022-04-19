import os
import uuid

import dask
import dask.dataframe as dd
import pytest
import s3fs

S3_REGION = "us-east-1"
storage_options = {"config_kwargs": {"region_name": S3_REGION}}


@pytest.fixture(scope="session")
def s3_bucket_name():
    return "dask-io"


@pytest.fixture(scope="session")
def s3():
    return s3fs.S3FileSystem(
        key=os.environ["AWS_ACCESS_KEY_ID"],
        secret=os.environ["AWS_SECRET_ACCESS_KEY"],
        client_kwargs={"region_name": S3_REGION},
    )


@pytest.fixture(scope="session")
def s3_stability_url(s3, s3_bucket_name):
    # Unique, because multiple tests are accessing the bucket
    stability_url = f"{s3_bucket_name}/stability-{uuid.uuid4().hex[:8]}"

    from pprint import pprint

    pprint(s3.ls(s3_bucket_name))

    try:
        s3.makedirs(stability_url)
        yield stability_url

    finally:
        s3.rm(stability_url, recursive=True)


@pytest.fixture
def s3_stability_write_url(s3, s3_stability_url):
    # Unique, because multiple tests are accessing the bucket
    write_url = f"{s3_stability_url}/write-{uuid.uuid4().hex[:8]}"

    try:
        s3.makedirs(write_url)
        yield write_url
    finally:
        s3.rm(write_url, recursive=True)


@pytest.fixture
def shuffle_dataset(small_client, s3_stability_url):
    """Produces a ~80GB dataset which is about the memory limit of the cluster"""
    df = dask.datasets.timeseries(
        start="2000-01-01", end="2000-12-31", freq="50ms", partition_freq="1D"
    )

    write = df.to_parquet(
        f"s3://{s3_stability_url}",
        compute=False,
        overwrite=True,
        storage_options=storage_options,
    )
    size = df.memory_usage(index=True).sum() / (1024.0**3)
    size, _ = dask.compute(write, size)

    yield dd.read_parquet(f"s3://{s3_stability_url}", storage_options=storage_options)


def test_shuffle_simple(shuffle_dataset, s3_stability_write_url):
    sdf = shuffle_dataset.shuffle(on="x")
    write = sdf.to_parquet(
        f"s3://{s3_stability_write_url}",
        compute=False,
        overwrite=True,
        storage_options=storage_options,
    )
    dask.compute(write)
