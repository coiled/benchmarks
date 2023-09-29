from __future__ import annotations

import dask.array as da
import pytest

from ..utils_test import run_up_to_nthreads, wait

pytest.importorskip("zarr")


@pytest.fixture(scope="module")
def zarr_dataset():
    # shape = (2000, 2000, 2000)
    # chunks = (200, 200, 200)
    # Compresses to ~42% of its original size (tested on lz4 4.0)
    store = (
        "s3://coiled-runtime-ci/synthetic-zarr/synth_random_int_array_2000_cubed.zarr"
    )
    return da.from_zarr(store)


@pytest.fixture(scope="module")
def cmip6():
    xarray = pytest.importorskip("xarray")
    pytest.importorskip("cftime")

    store = "s3://coiled-runtime-ci/CMIP6/CMIP/AS-RCEC/TaiESM1/1pctCO2/r1i1p1f1/Amon/zg/gn/v20200225/"
    return xarray.open_dataset(store, engine="zarr", chunks={})


@run_up_to_nthreads("small", 100, reason="fixed dataset")
@pytest.mark.parametrize("threshold", [50, 100, 200, 255])
def test_filter_then_average(client, zarr_dataset, threshold):
    """Compute the mean for increasingly sparse boolean filters of an array"""
    a = zarr_dataset[zarr_dataset > threshold].mean()
    wait(a, client, 300)


@run_up_to_nthreads("small", 50, reason="fixed dataset")
@pytest.mark.client("small")
@pytest.mark.parametrize("N", [700, 75, 1])
def test_access_slices(client, zarr_dataset, N):
    """Accessing just a few chunks of a zarr array should be quick"""
    a = zarr_dataset[:N, :N, :N]
    wait(a, client, 300)


@run_up_to_nthreads("small", 50, reason="fixed dataset")
@pytest.mark.client("small")
def test_sum_residuals(client, zarr_dataset):
    """Compute reduce, then map, then reduce again"""
    a = (zarr_dataset - zarr_dataset.mean(axis=0)).sum()
    wait(a, client, 300)


@run_up_to_nthreads("small", 50, reason="fixed dataset")
@pytest.mark.client("small")
def test_select_scalar(client, cmip6):
    ds = cmip6.isel({"lat": 20, "lon": 40, "plev": 5, "time": 1234}).compute()
    assert ds.zg.shape == ()
    assert ds.zg.size == 1
