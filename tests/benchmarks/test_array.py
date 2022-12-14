from __future__ import annotations

import time

import dask.array as da
import distributed
import numpy as np
import pytest
import xarray as xr
from dask.utils import format_bytes, parse_bytes
from packaging.version import Version

from ..utils_test import (
    arr_to_devnull,
    cluster_memory,
    run_up_to_nthreads,
    scaled_array_shape,
    scaled_array_shape_quadratic,
    wait,
)


@pytest.fixture(scope="module")
def zarr_dataset():
    s3_uri = (
        "s3://coiled-runtime-ci/synthetic-zarr/"
        "synth_random_int_array_2000_cubed.zarr"
    )
    return da.from_zarr(s3_uri)


def print_size_info(memory: int, target_nbytes: int, *arrs: da.Array) -> None:
    print(
        f"Cluster memory: {format_bytes(memory)}, "
        f"target data size: {format_bytes(target_nbytes)}"
    )
    for i, arr in enumerate(arrs, 1):
        print(
            f"Input {i}: {format_bytes(arr.nbytes)} - {arr.npartitions} "
            f"{format_bytes(arr.blocks[(0,) * arr.ndim].nbytes)} chunks"
        )


def test_anom_mean(small_client):
    # From https://github.com/dask/distributed/issues/2602#issuecomment-498718651

    memory = cluster_memory(small_client)  # 76.66 GiB
    target_nbytes = memory // 2
    data = da.random.random(
        scaled_array_shape(target_nbytes, ("x", "10MiB")),
        chunks=(1, parse_bytes("10MiB") // 8),
    )
    print_size_info(memory, target_nbytes, data)
    # 38.32 GiB - 3925 10.00 MiB chunks

    ngroups = data.shape[0] // 100
    arr = xr.DataArray(
        data,
        dims=["time", "x"],
        coords={"day": ("time", np.arange(data.shape[0]) % ngroups)},
    )

    clim = arr.groupby("day").mean(dim="time")
    anom = arr.groupby("day") - clim
    anom_mean = anom.mean(dim="time")

    wait(anom_mean, small_client, 10 * 60)


@pytest.mark.parametrize(
    "speed,chunk_shape",
    [
        ("fast", "thin"),
        ("slow", "thin"),
        ("slow", "square"),
    ],
)
def test_basic_sum(small_client, speed, chunk_shape):
    """From https://github.com/dask/distributed/pull/4864

    n-step map-reduce:

    1. map: create huge amounts of random data (5x the available memory on the whole
       cluster).
       The chunking is either by single column ("thin") or square; chunk size is 100MiB
       per chunk in both cases.
       Tasks in this group terminate almost instantly.

    2. map: in the "slow" use case, sleep for 0.5s after generating each 100MiB chunk.
       This step simulates e.g. a quite fast read from storage or midly intensive
       in-place computation. This step is skipped in the "fast" use case.

    3. map: first stage of dask.array.Array.sum(axis=1). Reduce each chunk individually
       along the columns. In case of "thin" chunks, this is a no-op.
       In case of "square" chunks, chunk sizes are negligible after this step.

    4. reduce: sum groups of <split_every> (default 4) contiguous chunks, thus
       obtaining 1/4th of the original chunks.

       In the "thin" chunk configuration, this step heavily relies on co-assignment -
       that is, it relies on the 4 nearby chunks to have been generated (most times) on
       the same worker and not need to be transferred across the network. Without
       co-assignment, this will result in up to the whole map output to be transferred
       across the network.

    5. reduce: recursively repeat step 4 until only one chunk remains.

    See dask/array/reductions.py for the implementation of recursive reductions.
    """
    if chunk_shape == "thin":
        chunks = (-1, 1)  # 100 MiB single-column chunks
    else:
        chunks = (3350, 3925)  # 100.32 MiB square-ish chunks

    memory = cluster_memory(small_client)  # 76.66 GiB
    target_nbytes = memory * 5
    data = da.zeros(
        scaled_array_shape(target_nbytes, ("100MiB", "x")),
        chunks=chunks,
    )
    print_size_info(memory, target_nbytes, data)
    # 383.30 GiB - 3925 100.00 MiB chunks (if thin)
    # 383.30 GiB - 3913 100.32 MiB chunks (if square)

    def slow_map(x):
        time.sleep(0.5)
        # Do not return the input by reference, as it would trigger an edge case where
        # managed memory is double counted
        return x.copy()

    if speed == "slow":
        data = data.map_blocks(slow_map)

    result = da.sum(data, axis=1)

    wait(result, small_client, 10 * 60)


@pytest.mark.skip(
    "fails in actual CI; see https://github.com/coiled/coiled-runtime/issues/253"
)
def test_climatic_mean(small_client):
    # From https://github.com/dask/distributed/issues/2602#issuecomment-535009454

    memory = cluster_memory(small_client)  # 76.66 GiB
    target_nbytes = memory * 2
    chunks = (1, 1, 96, 21, 90, 144)
    shape = (28, "x", 96, 21, 90, 144)
    data = da.random.random(scaled_array_shape(target_nbytes, shape), chunks=chunks)
    print_size_info(memory, target_nbytes, data)
    # 152.62 GiB - 784 199.34 MiB chunks

    array = xr.DataArray(
        data,
        dims=["ensemble", "init_date", "lat", "lead_time", "level", "lon"],
        # coords={"init_date": pd.date_range(start="1960", periods=arr.shape[1])},
        coords={"init_date": np.arange(data.shape[1]) % 10},
    )
    # arr_clim = array.groupby("init_date.month").mean(dim="init_date")
    arr_clim = array.groupby("init_date").mean(dim="init_date")

    wait(arr_clim, small_client, 15 * 60)


def test_vorticity(small_client):
    # From https://github.com/dask/distributed/issues/6571

    memory = cluster_memory(small_client)  # 76.66 GiB
    target_nbytes = int(memory * 0.85)
    shape = scaled_array_shape(target_nbytes, (5000, 5000, "x"))

    u = da.random.random(shape, chunks=(5000, 5000, 1))
    v = da.random.random(shape, chunks=(5000, 5000, 1))
    print_size_info(memory, target_nbytes, u, v)
    # Input 1: 65.19 GiB - 350 190.73 MiB chunks
    # Input 2: 65.19 GiB - 350 190.73 MiB chunks

    dx = da.random.random((5001, 5000), chunks=(5001, 5000))
    dy = da.random.random((5001, 5000), chunks=(5001, 5000))

    def pad_rechunk(arr):
        """
        Pad a single element onto the end of arr, then merge the 1-element long chunk
        created back in.

        This operation complicates each chain of the graph enough so that the scheduler
        no longer recognizes the overall computation as blockwise, but doesn't actually
        change the overall topology of the graph, or the number of chunks along any
        dimension of the array.

        This is motivated by the padding operation we do in xGCM, see

        https://xgcm.readthedocs.io/en/latest/grid_ufuncs.html#automatically-applying-boundary-conditions
        https://github.com/xgcm/xgcm/blob/fe860f96bbaa7293142254f48663d71fb97a4f36/xgcm/grid_ufunc.py#L871
        """

        padded = da.pad(arr, pad_width=[(0, 1), (0, 0), (0, 0)], mode="wrap")
        old_chunks = padded.chunks
        new_chunks = list(old_chunks)
        new_chunks[0] = 5001
        rechunked = da.rechunk(padded, chunks=new_chunks)
        return rechunked

    up = pad_rechunk(u)
    vp = pad_rechunk(v)
    result = dx[..., None] * up - dy[..., None] * vp

    wait(arr_to_devnull(result), small_client, 10 * 60)


def test_double_diff(small_client):
    # Variant of https://github.com/dask/distributed/issues/6597
    memory = cluster_memory(small_client)  # 76.66 GiB
    # FIXME https://github.com/coiled/coiled-runtime/issues/564
    #       this algorithm is supposed to scale linearly!
    shape = scaled_array_shape_quadratic(memory, "76.66 GiB", ("x", "x"))

    a = da.random.random(shape, chunks="20MiB")
    b = da.random.random(shape, chunks="20MiB")
    print_size_info(memory, memory, a, b)

    diff = a[1:, 1:] - b[:-1, :-1]
    wait(arr_to_devnull(diff), small_client, 10 * 60)


def test_dot_product(small_client):
    memory = cluster_memory(small_client)  # 76.66 GiB
    shape = scaled_array_shape_quadratic(memory // 17, "4.5 GiB", ("x", "x"))
    a = da.random.random(shape, chunks="128 MiB")
    print_size_info(memory, memory // 17, a)
    # Input 1: 4.51 GiB - 49 128.00 MiB chunks

    b = (a @ a.T).sum().round(3)
    wait(b, small_client, 10 * 60)


@pytest.mark.xfail(
    Version(distributed.__version__) < Version("2022.6.1"),
    reason="https://github.com/dask/distributed/issues/6624",
)
def test_map_overlap_sample(small_client):
    """
    This is from Napari like workloads where they have large images and
    commonly use map_overlap.  They care about rapid (sub-second) access to
    parts of a large array.

    At the time of writing, data creation took 300ms and the
    map_overlap/getitem, took 2-3s
    """
    x = da.random.random((10000, 10000), chunks=(50, 50))  # 40_000 19.5 kiB chunks
    y = x.map_overlap(lambda x: x, depth=1)
    y[5000:5010, 5000:5010].compute()


@run_up_to_nthreads("small_cluster", 100, reason="fixed dataset")
@pytest.mark.parametrize("threshold", [50, 100, 200, 255])
def test_filter_then_average(threshold, zarr_dataset, small_client):
    """
    Compute the mean for increasingly sparse boolean filters of an array
    """
    zarr_dataset[zarr_dataset > threshold].mean().compute()


@run_up_to_nthreads("small_cluster", 50, reason="fixed dataset")
@pytest.mark.parametrize("N", [700, 75, 1])
def test_access_slices(N, zarr_dataset, small_client):
    """
    Accessing just a few chunks of a zarr array should be quick
    """
    distributed.wait(zarr_dataset[:N, :N, :N].persist())


@run_up_to_nthreads("small_cluster", 50, reason="fixed dataset")
def test_sum_residuals(zarr_dataset, small_client):
    """
    Simnple test to that computes as reduction, the array op, the reduction again
    """
    (zarr_dataset - zarr_dataset.mean(axis=0)).sum().compute()
