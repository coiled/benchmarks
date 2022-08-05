import uuid

import dask
import dask.array as da
import pytest
from coiled.v2 import Cluster
from dask.distributed import Client, wait


@pytest.mark.stability
@pytest.mark.parametrize("keep_around", (True, False))
def test_spilling(keep_around):
    with dask.config.set({"distributed.scheduler.allowed-failures": 0}):
        with Cluster(
            name=f"test_spill-{uuid.uuid4().hex}",
            n_workers=5,
            worker_disk_size=55,
            wait_for_workers=True,
        ) as cluster:
            with Client(cluster) as client:
                arr = da.random.random((200, 2**27)).persist()  # 200 GiB
                wait(arr)
                fut = client.compute(arr.sum())
                if not keep_around:
                    del arr
                wait(fut)
