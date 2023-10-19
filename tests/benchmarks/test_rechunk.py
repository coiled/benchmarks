from __future__ import annotations

import time

import dask.array as da
import numpy as np
import pytest

from typing import Any

import dask
from ..utils_test import run_up_to_nthreads, cluster_memory, scaled_array_shape
from dask.utils import format_bytes, parse_bytes

@pytest.fixture(params=["8 MiB", "128 MiB"])
def chunksize(request):
    return request.param


@pytest.fixture
def configure_chunksize(chunksize):
    with dask.config.set({"array.chunk-size": chunksize}):
        yield

def test_tiles_to_rows(small_client, memory_multiplier, configure_chunksize, configure_rechunking):
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * memory_multiplier, ("x", "x"))

    rng = da.random.default_rng()
    arr = rng.random(shape, chunks="auto")
    arr.rechunk((-1, "auto")).sum().compute()


def test_swap_axes(small_client, memory_multiplier, configure_chunksize, configure_rechunking):
    memory = cluster_memory(small_client)
    shape = scaled_array_shape(memory * memory_multiplier, ("x", "x"))

    rng = da.random.default_rng()
    arr = rng.random(shape, chunks=(-1, "auto"))
    arr.rechunk(("auto", -1)).sum().compute()
