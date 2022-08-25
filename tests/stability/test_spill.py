import uuid

import dask.array as da
import pytest
from coiled.v2 import Cluster
from dask.distributed import Client, wait


@pytest.fixture(scope="module")
def spill_cluster():
    with Cluster(
        f"spill-{uuid.uuid4().hex[:8]}",
        n_workers=5,
        package_sync=True,
        worker_disk_size=55,
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
        yield cluster


@pytest.fixture
def spill_client(spill_cluster, sample_memory, benchmark_time):
    with Client(spill_cluster) as client:
        spill_cluster.scale(5)
        client.wait_for_workers(5)
        client.restart()
        with sample_memory(client), benchmark_time:
            yield client


@pytest.mark.stability
@pytest.mark.parametrize("keep_around", (True, False))
def test_spilling(spill_client, keep_around):
    arr = da.random.random((200, 2**27)).persist()  # 200 GiB
    wait(arr)
    fut = spill_client.compute(arr.sum())
    if not keep_around:
        del arr
    wait(fut)


@pytest.mark.skip(
    reason="Skip until https://github.com/coiled/feedback/issues/185 is resolved."
)
@pytest.mark.stability
def test_tensordot_stress(spill_client):
    a = da.random.random((48 * 1024, 48 * 1024))  # 18 GiB
    b = (a @ a.T).sum().round(3)
    fut = spill_client.compute(b)
    wait(fut)
    assert fut.result()
