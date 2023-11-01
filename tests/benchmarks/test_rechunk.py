from __future__ import annotations

import dask
import dask.array as da
import pytest
from dask.utils import parse_bytes

from ..conftest import P2P_MEMORY_AVAILABLE, P2P_RECHUNK_AVAILABLE
from ..utils_test import cluster_memory, scaled_array_shape


@pytest.fixture(
    params=[
        "tasks",
        pytest.param(
            "p2p-disk",
            marks=pytest.mark.skipif(
                not P2P_RECHUNK_AVAILABLE, reason="p2p rechunk not available"
            ),
        ),
        pytest.param(
            "p2p-memory",
            marks=pytest.mark.skipif(
                not P2P_MEMORY_AVAILABLE, reason="Requires p2p in-memory shuffle"
            ),
        ),
    ]
)
def configure_rechunking(request, memory_multiplier):
    if request.param == "tasks":
        with dask.config.set({"array.rechunk.method": "tasks"}):
            yield
    else:
        disk = "disk" in request.param
        if not disk and memory_multiplier > 0.4:
            pytest.skip("Out of memory")
        with dask.config.set(
            {
                "array.rechunk.method": "p2p",
                "distributed.p2p.disk": disk,
            }
        ):
            yield


@pytest.fixture(params=["8 MiB", "128 MiB"])
def configure_chunksize(request, memory_multiplier):
    if memory_multiplier > 0.4 and parse_bytes(request.param) < parse_bytes("64 MiB"):
        pytest.skip("too slow")

    with dask.config.set({"array.chunk-size": request.param}):
        yield


def test_tiles_to_rows(
    # Order matters: don't initialize client when skipping test
    memory_multiplier,
    configure_chunksize,
    configure_rechunking,
    small_client,
):
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * memory_multiplier, ("x", "x"))

    arr = da.random.random(shape, chunks="auto")
    arr.rechunk((-1, "auto")).sum().compute()


def test_swap_axes(
    # Order matters: don't initialize client when skipping test
    memory_multiplier,
    configure_chunksize,
    configure_rechunking,
    small_client,
):
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * memory_multiplier, ("x", "x"))

    arr = da.random.random(shape, chunks=(-1, "auto"))
    arr.rechunk(("auto", -1)).sum().compute()
