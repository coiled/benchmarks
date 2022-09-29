from __future__ import annotations

import dask.array as da
import distributed
import numpy as np
import pytest
import xarray as xr
from dask.utils import format_bytes, parse_bytes
from packaging.version import Version

from ..utils_test import arr_to_devnull, cluster_memory, scaled_array_shape, wait


def print_size_info(memory: int, target_nbytes: int, *arrs: da.Array) -> None:
    print(
        f"Cluster memory: {format_bytes(memory)}, target data size: {format_bytes(target_nbytes)}"
    )
    for i, arr in enumerate(arrs, 1):
        print(
            f"Input {i}: {format_bytes(arr.nbytes)} - "
            f"{arr.npartitions} {format_bytes(arr.blocks[(0,) * arr.ndim].nbytes)} chunks"
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


def test_basic_sum(small_client):
    # From https://github.com/dask/distributed/pull/4864

    memory = cluster_memory(small_client)  # 76.66 GiB
    target_nbytes = memory * 5
    data = da.random.randint(
        0,
        255,
        scaled_array_shape(target_nbytes, ("100MiB", "x")),
        chunks=(parse_bytes("100MiB") // 8, 1),
    )
    print_size_info(memory, target_nbytes, data)
    # 383.20 GiB - 3924 100.00 MiB chunks

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

    # TODO switch back to chunksizes in the `chunks=` argument everywhere
    #  when https://github.com/dask/dask/issues/9488 is fixed
    cs = int((parse_bytes("20 MiB") / 8) ** (1 / 2))
    a = da.random.random(scaled_array_shape(memory, ("x", "x")), chunks=(cs, cs))
    b = da.random.random(scaled_array_shape(memory, ("x", "x")), chunks=(cs, cs))
    print_size_info(memory, memory, a, b)

    diff = a[1:, 1:] - b[:-1, :-1]
    wait(arr_to_devnull(diff), small_client, 10 * 60)


def test_dot_product(small_client):
    a = da.random.random((24 * 1024, 24 * 1024), chunks="128 MiB")  # 4.5 GiB
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
    x = da.random.random((10000, 10000), chunks=(50, 50))

    y = x.map_overlap(lambda x: x, depth=1)
    y[5000:5010, 5000:5010].compute()


@pytest.fixture(
    scope="module",
    params=[500, 1000, 2000],
    ids=["small", "medium", "large"]
)
def zarr_dataset(request):
    s3_uri = (
        f"s3://coiled-runtime-ci/synthetic-zarr/"
        f"synth_random_int_array_{request.param}_cubed.zarr"
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
