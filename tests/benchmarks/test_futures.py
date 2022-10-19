import numpy as np
from dask.distributed import as_completed, wait
from distributed.utils_test import inc, slowdec, slowinc


def test_single_future(small_client):
    """How quickly can we run a simple computation?"""
    for i in range(400):
        small_client.submit(inc, i).result()


def test_large_map(small_client):
    """What's the overhead of map these days?"""
    futures = small_client.map(inc, range(100_000))
    wait(futures)


def test_large_map_first_work(small_client):
    """
    Large maps are fine, but it's pleasant to see work start immediately.
    We have a batch_size keyword that should work here but it's not on by default.
    Maybe it should be.
    """
    futures = small_client.map(inc, range(100_000))
    for _ in as_completed(futures):
        return


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
