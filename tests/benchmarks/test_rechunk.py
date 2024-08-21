from __future__ import annotations

import dask
import dask.array as da
import pytest

from ..conftest import requires_p2p_memory, requires_p2p_rechunk
from ..utils_test import cluster_memory, scaled_array_shape, wait


@pytest.fixture(params=["8.5 MiB", "auto"])
def input_chunk_size(request):
    return request.param


@pytest.fixture(
    params=[
        pytest.param("tasks", marks=pytest.mark.shuffle_tasks),
        pytest.param("p2p-disk", marks=[pytest.mark.shuffle_p2p, requires_p2p_rechunk]),
        pytest.param(
            "p2p-memory", marks=[pytest.mark.shuffle_p2p, requires_p2p_memory]
        ),
    ]
)
def configure_rechunking_in_memory(request):
    if request.param == "tasks":
        with dask.config.set({"array.rechunk.method": "tasks"}):
            yield
    else:
        disk = "disk" in request.param
        with dask.config.set(
            {
                "array.rechunk.method": "p2p",
                "distributed.p2p.disk": disk,
            }
        ):
            yield


@pytest.fixture(
    params=[
        pytest.param("tasks", marks=pytest.mark.shuffle_tasks),
        pytest.param("p2p", marks=[pytest.mark.shuffle_p2p, requires_p2p_rechunk]),
    ]
)
def configure_rechunking_out_of_core(request):
    if request.param == "tasks":
        with dask.config.set({"array.rechunk.method": "tasks"}):
            yield
    else:
        with dask.config.set(
            {
                "array.rechunk.method": "p2p",
                "distributed.p2p.disk": True,
            }
        ):
            yield


def test_tiles_to_rows(
    # Order matters: don't initialize client when skipping test
    input_chunk_size,
    configure_rechunking_in_memory,
    small_client,
):
    """2D array sliced into square tiles becomes sliced by columns.
    This use case can be broken down into N independent problems.
    In task rechunk, this generates O(N) intermediate tasks and graph edges.
    """
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * 1.5, ("x", "x"))

    a = da.random.random(shape, chunks=input_chunk_size)
    a = a.rechunk((-1, "auto")).sum()
    wait(a, small_client, timeout=600)


def test_swap_axes_in_memory(
    # Order matters: don't initialize client when skipping test
    input_chunk_size,
    small_client,
):
    """2D array sliced by columns becomes sliced by rows.
    This is an N-to-N problem, so grouping into sub-problems is impossible.
    In task rechunk, this generates O(N^2) intermediate tasks and graph edges.
    """
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * 0.5, ("x", "x"))

    a = da.random.random(shape, chunks=(-1, input_chunk_size))
    a = a.rechunk(("auto", -1)).sum()
    wait(a, small_client, timeout=600)


def test_swap_axes_out_of_core(
    # Order matters: don't initialize client when skipping test
    small_client,
):
    """2D array sliced by columns becomes sliced by rows.
    This is an N-to-N problem, so grouping into sub-problems is impossible.
    In task rechunk, this generates O(N^2) intermediate tasks and graph edges.
    """
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * 1.5, ("x", "x"))

    a = da.random.random(shape, chunks=(-1, "auto"))
    a = a.rechunk(("auto", -1)).sum()
    wait(a, small_client, timeout=600)


def test_adjacent_groups(
    # Order matters: don't initialize client when skipping test
    input_chunk_size,
    small_client,
):
    """M-to-N use case, where each input task feeds into a localized but substantial
    subset of the output tasks, with partial interaction between adjacent zones.
    """
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * 1.5, ("x", 10, 10_000))

    a = da.random.random(shape, chunks=(input_chunk_size, 2, 5_000))
    a = a.rechunk(("auto", 5, 10_000)).sum()
    wait(a, small_client, timeout=600)


def test_heal_oversplit(
    # Order matters: don't initialize client when skipping test
    small_client,
):
    """rechunk() is used to heal a situation where chunks are too small.
    This is a trivial N-to-1 reduction step that gets no benefit from p2p rechunking.
    """
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * 1.5, ("x", "x"))
    # Avoid exact n:1 rechunking, which would be a simpler special case.
    # Dask should be smart enough to avoid splitting input chunks out to multiple output
    # chunks.
    a = da.random.random(shape, chunks="8.5 MiB")
    a = a.rechunk("128 MiB").sum()
    wait(a, small_client, timeout=600)
