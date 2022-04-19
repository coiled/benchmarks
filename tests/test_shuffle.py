import os
import uuid
import warnings

import dask
import dask.dataframe as dd
import pytest
import s3fs

S3_REGION = "us-east-1"
storage_options = {"config_kwargs": {"region_name": S3_REGION}}


@pytest.fixture
def s3_bucket_name():
    return "S3-dask-io-read-write"


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
    stability_url = f"s3://{s3_bucket_name}/stability-{uuid.uuid4().hex[:8]}"

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
    # NOTE(sjperkins)
    # I think the dataset and worker memory limits are related for this test
    # as the dataset is large enough to provoke some spilling when it is
    # persisted. Warn if the worker memory limits change egregiously.
    # Alternatively, a separate cluster fixture may be appropriate
    # for shuffle tests.
    workers = [w for w in small_client.scheduler_info()["workers"].values()]
    mem_limit = [w["memory_limit"] for w in workers.values()]
    ONE_GB = 1024.0**3

    if any(wm < 7.5 * ONE_GB for wm in mem_limit):
        warnings.warn("Worker memory limit < 7.5GB")

    if any(wm > 8.5 * ONE_GB for wm in mem_limit):
        warnings.warn("Worker memory limit > 8.5GB")

    if sum(mem_limit) > 85 * ONE_GB:
        warnings.warn("Total worker memory limit > 85GB")

    if sum(mem_limit) < 75 * ONE_GB:
        warnings.warn("Total worker memory limit < 75GB")

    df = dask.datasets.timeseries(
        start="2000-01-01", end="2000-12-31", freq="50ms", partition_freq="1D"
    )

    write = df.to_parquet(
        s3_stability_url, compute=False, overwrite=True, storage_options=storage_options
    )
    size = df.memory_usage(index=True).sum() / ONE_GB

    size, _ = dask.compute(write, size)

    yield dd.read_parquet(s3_stability_url, storage_options=storage_options)


def test_shuffle_simple(shuffle_dataset, write_url):
    sdf = shuffle_dataset.shuffle(on="x")
    write = sdf.to_parquet(
        write_url,
        compute=False,
        overwrite=True,
        storage_options=storage_options,
    )
    dask.compute(write)
