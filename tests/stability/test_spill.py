import uuid

import dask
import dask.array as da
import pytest
from coiled.v2 import Cluster
from dask.distributed import Client, wait


@pytest.mark.stability
def test_spilling():
    with dask.config.set({"distributed.scheduler.allowed-failures": 0}):
        with Cluster(
            name=f"test_spilling-{uuid.uuid4().hex}",
            n_workers=5,
            worker_disk_size=50,
            wait_for_workers=True,
        ) as cluster:
            with Client(cluster) as client:
                arr = da.random.random((200, 2**27))  # 200 GiB
                wait(arr.persist())
                fut = client.compute(arr.sum())
                del arr
                wait(fut)
