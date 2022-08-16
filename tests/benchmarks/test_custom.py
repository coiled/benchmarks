import random
import time

from dask import delayed
from dask.utils import parse_bytes

from ..utils_test import wait


def test_jobqueue(small_client):
    # Just using dask to run lots of embarrassingly-parallel CPU-bound tasks as fast as possible
    nthreads = sum(
        w["nthreads"] for w in small_client.scheduler_info()["workers"].values()
    )
    max_runtime = 120
    max_sleep = 3
    n_tasks = round(max_runtime / max_sleep * nthreads)

    @delayed(pure=True)
    def task(i: int) -> int:
        stuff = "x" * parse_bytes("400MiB")
        time.sleep(random.uniform(0, max_sleep))
        del stuff
        return i

    tasks = [task(i) for i in range(n_tasks)]
    result = delayed(sum)(*tasks)  # just so we have a single object

    wait(
        result,
        small_client,
        max_runtime * 1.15,
    )
