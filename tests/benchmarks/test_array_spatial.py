from __future__ import annotations

import dask.array as da
import pytest


@pytest.fixture(scope="module")
def zarr_dataset():
    s3_uri = (
        "s3://coiled-runtime-ci/synthetic-zarr/"
        "synth_random_int_array_2000_cubed.zarr"
    )
    return da.from_zarr(s3_uri)


@pytest.mark.parametrize("threshold", [50, 100, 200, 255])
def test_filter_then_average(threshold, zarr_dataset, small_client):
    _ = zarr_dataset[zarr_dataset > threshold].mean().compute()


@pytest.mark.parametrize("N", [500, 250, 50, 1])
def test_access_slices(N, zarr_dataset):
    _ = zarr_dataset[:N, :N, :N].compute()


def test_sum_residuals(zarr_dataset):
    _ = (zarr_dataset - zarr_dataset.mean(axis=0)).sum()


def test_map_overlap_extact(zarr_dataset):
    return (
        zarr_dataset[:, :, 0]
        .map_overlap(lambda x: x * 2, depth=2)[:50, 250:303]
        .compute()
    )
