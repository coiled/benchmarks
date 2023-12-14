from __future__ import annotations

import dask
import dask.array as da
import pytest
from dask.utils import parse_bytes

from ..conftest import requires_p2p_memory, requires_p2p_rechunk
from ..utils_test import cluster_memory, scaled_array_shape, wait


@pytest.fixture(
    params=[
        pytest.param("tasks", marks=pytest.mark.shuffle_tasks),
        pytest.param("p2p-disk", marks=[pytest.mark.shuffle_p2p, requires_p2p_rechunk]),
        pytest.param(
            "p2p-memory", marks=[pytest.mark.shuffle_p2p, requires_p2p_memory]
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

    a = da.random.random(shape, chunks="auto")
    a = a.rechunk((-1, "auto")).sum()
    wait(a, small_client, timeout=600)


def test_swap_axes(
    # Order matters: don't initialize client when skipping test
    memory_multiplier,
    configure_chunksize,
    configure_rechunking,
    small_client,
):
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * memory_multiplier, ("x", "x"))

    a = da.random.random(shape, chunks=(-1, "auto"))
    a = a.rechunk(("auto", -1)).sum()
    wait(a, small_client, timeout=600)


def test_partial_rechunk(
    # Order matters: don't initialize client when skipping test
    memory_multiplier,
    configure_chunksize,
    configure_rechunking,
    small_client,
):
    """Test a complete, varied use case for partial rechunking:

    test_swap_axes
        This is a n-to-n problem, so partial rechunking is impossible.
        Each random_sample task feeds into exactly one rechunk_transfer task.
        There is a single shuffle-barrier for the whole graph.
    test_tiles_to_rows
        P2P rechunking is broken down into multiple partial rechunks.
        Eeach random_sample task feeds exactly into two rechunk_slice tasks belonging
        to two different partial rechunks (different shuffle-barrier tasks).
        Each rechunk_slice task feeds into exactly one rechunk_transfer task.
    test_partial_rechunk
        P2P rechunking is broken down into multiple partial rechunks.
        random_sample tasks:
        - sometimes feed into two rechunk_slice tasks belonging to two different
          partial rechunks
        - sometimes feed directly into a rechunk task (without barrier)
    """
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * memory_multiplier, ("x", 10, 10_000))

    a = da.random.random(shape, chunks=("auto", 2, 5_000))
    a = a.rechunk(("auto", 5, 10_000)).sum()
    wait(a, small_client, timeout=600)


def test_heal_oversplit(
    # Order matters: don't initialize client when skipping test
    memory_multiplier,
    configure_rechunking,
    small_client,
):
    """rechunk() is used to heal a situation where chunks are too small.
    This use case gets no benefit whatsoever from P2P rechunking.
    Measure the performance discrepancy between p2p and task rechunk.
    """
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * memory_multiplier, ("x", "x"))
    # Avoid exact n:1 rechunking, which would be a simpler special case.
    # Dask should be smart enough to avoid splitting input chunks over multiple output
    # chunks.
    with dask.config.set({"array.chunk-size": "8.5 MiB"}):
        a = da.random.random(shape, chunks="auto")
    with dask.config.set({"array.chunk-size": "128 MiB"}):
        a = a.rechunk("auto").sum()
    wait(a, small_client, timeout=600)
