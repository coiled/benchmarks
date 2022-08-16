import uuid

import dask.array as da
import pytest
from coiled.v2 import Cluster
from dask.distributed import Client, wait


@pytest.mark.stability
@pytest.mark.parametrize("keep_around", (True, False))
def test_spilling(keep_around):
    with Cluster(
        name=f"test_spill-{uuid.uuid4().hex}",
        n_workers=5,
        worker_disk_size=55,
        worker_vm_types=["t3.medium"],
        wait_for_workers=True,
        environ={
            # Note: We set allowed-failures to ensure that no tasks are not retried
            #  upon ungraceful shutdown behavior during adaptive scaling
            #  but we receive a KilledWorker() instead.
            "DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES": "0"
        },
    ) as cluster:
        with Client(cluster) as client:
            arr = da.random.random((200, 2**27)).persist()  # 200 GiB
            wait(arr)
            fut = client.compute(arr.sum())
            if not keep_around:
                del arr
            wait(fut)


@pytest.mark.stability
def test_tensordot_stress():
    with Cluster(
        name=f"test_spill-{uuid.uuid4().hex}",
        n_workers=5,
        worker_disk_size=45,
        worker_vm_types=["t3.medium"],
        wait_for_workers=True,
        environ={
            # Note: We set allowed-failures to ensure that no tasks are not retried
            #  upon ungraceful shutdown behavior during adaptive scaling
            #  but we receive a KilledWorker() instead.
            "DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES": "0",
            # We need to limit the number of connections to avoid getting `oom-killed`.
            #  See https://github.com/coiled/coiled-runtime/pull/229#discussion_r946807049
            #  for a longer discussion
            "DASK_DISTRIBUTED__WORKER__CONNECTIONS__INCOMING": "1",
            "DASK_DISTRIBUTED__WORKER__CONNECTIONS__OUTGOING": "1",
        },
    ) as cluster:
        with Client(cluster) as client:
            a = da.random.random((48 * 1024, 48 * 1024))  # 18 GiB
            b = (a @ a.T).sum().round(3)
            fut = client.compute(b)
            wait(fut)
            assert fut.result()
