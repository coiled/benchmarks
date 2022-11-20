import uuid

import dask.array as da
import pytest
from coiled import Cluster
from dask.distributed import Client, wait
from toolz import merge


@pytest.fixture(scope="module")
def spill_cluster(dask_env_variables, gitlab_cluster_tags):
    with Cluster(
        f"spill-{uuid.uuid4().hex[:8]}",
        n_workers=5,
        package_sync=True,
        worker_vm_types=["m6id.large"],
        scheduler_vm_types=["m6i.xlarge"],
        wait_for_workers=True,
        backend_options={
            "spot": True,
            "spot_on_demand_fallback": True,
            "multizone": True,
        },
        environ=merge(
            dask_env_variables,
            {
                # Note: We set allowed-failures to ensure that no tasks are not retried
                # upon ungraceful shutdown behavior during adaptive scaling but we
                # receive a KilledWorker() instead.
                "DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES": "0",
                # We need to limit the number of connections to avoid getting
                # `oom-killed`. For a longer discussion, see
                # https://github.com/coiled/coiled-runtime/pull/229#discussion_r946807049
                "DASK_DISTRIBUTED__WORKER__CONNECTIONS__INCOMING": "1",
                "DASK_DISTRIBUTED__WORKER__CONNECTIONS__OUTGOING": "1",
            },
        ),
        tags=gitlab_cluster_tags,
    ) as cluster:
        yield cluster


@pytest.fixture
def spill_client(spill_cluster, upload_cluster_dump, benchmark_all):
    with Client(spill_cluster) as client:
        spill_cluster.scale(5)
        client.wait_for_workers(5)
        client.restart()
        with upload_cluster_dump(client), benchmark_all(client):
            yield client


@pytest.mark.stability
@pytest.mark.parametrize("keep_around", (True, False))
def test_spilling(spill_client, keep_around):
    arr = da.random.random((64, 2**27)).persist()  # 64 GiB, ~2x memory
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
