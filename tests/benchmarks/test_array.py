from __future__ import annotations

import time

import dask.array as da
import numpy as np
import pytest
from dask.core import flatten
from dask.utils import parse_bytes

from ..utils_test import (
    arr_to_devnull,
    cluster_memory,
    print_size_info,
    run_up_to_nthreads,
    scaled_array_shape,
    scaled_array_shape_quadratic,
    wait,
)


def _create_indexer(n, chunk_n):
    idx = np.arange(0, n)
    np.random.shuffle(idx)

    indexer = []
    for i in range(0, n, chunk_n):
        indexer.append(idx[i : i + chunk_n].tolist())
    return indexer


def test_take(small_client, new_array):
    n = 2000
    chunk_n = 250
    x = new_array((n, n, n), chunks=(chunk_n, chunk_n, chunk_n))
    indexer = list(flatten(_create_indexer(n, chunk_n)))
    x[:, indexer, :].sum().compute()


# def test_shuffle(small_client, new_array):
#     n = 2000
#     chunk_n = 250
#     x = new_array((n, n, n), chunks=(chunk_n, chunk_n, chunk_n))
#     indexer = _create_indexer(n, chunk_n)
#     x.shuffle(indexer, axis=1).sum().compute()
