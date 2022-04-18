import warnings

import dask
import pytest

BASE_URL = "s3://dask-io/stability"
storage_options = {"config_kwargs": {"region_name": "us-east-1"}}


@pytest.fixture
def shuffle_dataset(small_client):
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

    yield df
    # url = f"{BASE_URL}/test-data"
    # write = df.to_parquet(
    #     url, compute=False, overwrite=True, storage_options=storage_options
    # )
    # size = df.memory_usage(index=True).sum() / ONE_GB

    # size, _ = dask.compute(write, size)

    # yield dd.read_parquet(url, storage_options=storage_options)


def test_shuffle_simple(shuffle_dataset):
    sdf = shuffle_dataset.shuffle(on="x")
    dask.compute(sdf)
    # write = sdf.to_parquet(
    #     f"{BASE_URL}/simple",
    #     compute=False,
    #     overwrite=True,
    #     storage_options=storage_options,
    # )
    # dask.compute(write)
