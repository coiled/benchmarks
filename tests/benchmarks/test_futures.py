import numpy as np
import pytest
from dask.distributed import as_completed, wait
from distributed.utils_test import inc, slowdec, slowinc

from ..utils_test import run_up_to_nthreads


@run_up_to_nthreads("small_cluster", 50, reason="fixed dataset")
def test_single_future(small_client):
    """How quickly can we run a simple computation?
    Repeat the test a few times to get a more sensible
    cumulative measure.
    """
    for i in range(100):
        small_client.submit(inc, i).result()


@run_up_to_nthreads("small_cluster", 50, reason="fixed dataset")
@pytest.mark.parametrize("rootish", ["rootish", "non-rootish"])
def test_large_map(small_client, rootish):
    """What's the overhead of map these days?"""
    if rootish == "rootish":
        futures = small_client.map(inc, range(100_000))
    else:

        def inc_with_deps(i, deps):
            return i + 1

        deps = small_client.map(inc, range(5))
        futures = small_client.map(inc_with_deps, range(100_000), deps=deps)

    wait(futures)


@run_up_to_nthreads("small_cluster", 50, reason="fixed dataset")
@pytest.mark.skip(
    reason="Skip until https://github.com/coiled/benchmarks/issues/521 is fixed"
)
def test_large_map_first_work(small_client):
    """
    Large maps are fine, but it's pleasant to see work start immediately.
    We have a batch_size keyword that should work here but it's not on by default.
    Maybe it should be.
    """
    futures = small_client.map(inc, range(100_000))
    for _ in as_completed(futures):
        return


@run_up_to_nthreads("small_cluster", 100, reason="fixed dataset")
def test_memory_efficient(small_client):
    """
    We hope that we pipeline xs->ys->zs without keeping all of the xs in memory
    to start. This may not actually happen today.
    """
    xs = small_client.map(np.random.random, [20_000_000] * 100, pure=False)
    ys = small_client.map(slowinc, xs, delay=1)
    zs = small_client.map(slowdec, ys, delay=1)

    futures = as_completed(zs)
    del xs, ys, zs  # Don't keep references to intermediate results

    for _ in futures:  # pass through all futures, forget them immediately
        continue
