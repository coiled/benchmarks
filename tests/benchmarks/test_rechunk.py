from __future__ import annotations

import dask
import dask.array as da
import pytest

from ..utils_test import cluster_memory, scaled_array_shape


@pytest.fixture(
    params=[
        pytest.param(
            "8 MiB",
            marks=pytest.mark.skip(
                "FIXME: Skip for scheduled benchmarks due to runtime, uncomment on demand "
                "or once we have a more flexible system for running tests on different schedules."
            ),
        ),
        "128 MiB",
    ]
)
def chunksize(request):
    return request.param


@pytest.fixture
def configure_chunksize(chunksize):
    with dask.config.set({"array.chunk-size": chunksize}):
        yield


def test_tiles_to_rows(
    small_client, memory_multiplier, configure_chunksize, configure_rechunking
):
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * memory_multiplier, ("x", "x"))

    arr = da.random.random(shape, chunks="auto")
    arr.rechunk((-1, "auto")).sum().compute()


def test_swap_axes(
    small_client, memory_multiplier, configure_chunksize, configure_rechunking
):
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * memory_multiplier, ("x", "x"))

    arr = da.random.random(shape, chunks=(-1, "auto"))
    arr.rechunk(("auto", -1)).sum().compute()
