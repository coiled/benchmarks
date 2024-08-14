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
def chunk_size(request):
    return request.param


@pytest.fixture
def configure_chunksize(chunk_size, memory_multiplier):
    if memory_multiplier > 0.4 and parse_bytes(chunk_size) < parse_bytes("64 MiB"):
        pytest.skip("too slow")

    with dask.config.set({"array.chunk-size": chunk_size}):
        yield


@pytest.fixture
def input_chunk_size(chunk_size):
    return chunk_size


@pytest.fixture
def output_chunk_size(chunk_size):
    return chunk_size


def test_tiles_to_rows(
    # Order matters: don't initialize client when skipping test
    memory_multiplier,
    input_chunk_size,
    output_chunk_size,
    configure_rechunking,
    small_client,
):
    """2D array sliced into square tiles becomes sliced by columns.
    This use case can be broken down into N independent problems.
    In task rechunk, this generates O(N) intermediate tasks and graph edges.
    """
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * memory_multiplier, ("x", "x"))

    a = da.random.random(shape, chunks=input_chunk_size)
    a = a.rechunk((-1, output_chunk_size)).sum()
    wait(a, small_client, timeout=600)


def test_swap_axes(
    # Order matters: don't initialize client when skipping test
    memory_multiplier,
    configure_chunksize,
    input_chunk_size,
    output_chunk_size,
    configure_rechunking,
    small_client,
):
    """2D array sliced by columns becomes sliced by rows.
    This is an N-to-N problem, so grouping into sub-problems is impossible.
    In task rechunk, this generates O(N^2) intermediate tasks and graph edges.
    """
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * memory_multiplier, ("x", "x"))

    a = da.random.random(shape, chunks=(-1, input_chunk_size))
    a = a.rechunk((output_chunk_size, -1)).sum()
    wait(a, small_client, timeout=600)


def test_adjacent_groups(
    # Order matters: don't initialize client when skipping test
    memory_multiplier,
    input_chunk_size,
    output_chunk_size,
    configure_rechunking,
    small_client,
):
    """M-to-N use case, where each input task feeds into a localized but substantial
    subset of the output tasks, with partial interaction between adjacent zones.
    """
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * memory_multiplier, ("x", 10, 10_000))

    a = da.random.random(shape, chunks=(input_chunk_size, 2, 5_000))
    a = a.rechunk((output_chunk_size, 5, 10_000)).sum()
    wait(a, small_client, timeout=600)


def test_heal_oversplit(
    # Order matters: don't initialize client when skipping test
    memory_multiplier,
    configure_rechunking,
    small_client,
):
    """rechunk() is used to heal a situation where chunks are too small.
    This is a trivial N-to-1 reduction step that gets no benefit from p2p rechunking.
    """
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * memory_multiplier, ("x", "x"))
    # Avoid exact n:1 rechunking, which would be a simpler special case.
    # Dask should be smart enough to avoid splitting input chunks out to multiple output
    # chunks.
    a = da.random.random(shape, chunks="8.5 MiB")
    a = a.rechunk("128 MiB").sum()
    wait(a, small_client, timeout=600)
