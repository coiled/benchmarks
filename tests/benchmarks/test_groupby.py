import uuid

import distributed
import pytest
from coiled.v2 import Cluster

from tests.utils_test import cluster_memory, timeseries_of_size, wait


@pytest.fixture(scope="function")
def local_cluster():
    with distributed.LocalCluster() as cluster:
        yield cluster


@pytest.fixture(scope="function")
def local_client(local_cluster, sample_memory, benchmark_time):
    with distributed.Client(local_cluster) as client:
        with sample_memory(client), benchmark_time:
            yield client


N_WORKERS = 15


@pytest.fixture(scope="module")
def groupby_cluster():
    with Cluster(
        f"groupby-{uuid.uuid4().hex[:8]}",
        n_workers=N_WORKERS,
        worker_vm_types=["m5.xlarge"],
        scheduler_vm_types=["m5.xlarge"],
        package_sync=True,
    ) as cluster:
        yield cluster


@pytest.fixture
def groupby_client(groupby_cluster, sample_memory, benchmark_time):
    with distributed.Client(groupby_cluster) as client:
        groupby_cluster.scale(N_WORKERS)
        client.wait_for_workers(N_WORKERS)
        client.restart()
        with sample_memory(client), benchmark_time:
            yield client


@pytest.mark.parametrize("shuffle", [False, "tasks", "p2p"])
@pytest.mark.parametrize(
    "n_groups", [1000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000]
)
@pytest.mark.parametrize("split_out", [1, 4, 8])
def test_groupby(groupby_client, split_out, n_groups, shuffle):
    ddf = timeseries_of_size(
        cluster_memory(groupby_client) // 4,
        id_maximum=n_groups,
    )
    agg = ddf.groupby("id").agg(
        {"x": "sum", "y": "mean"},
        split_every=16,  # Note that the default split_every is a bit small when split_out>1
        split_out=split_out,
        shuffle=shuffle,
    )
    wait(agg, groupby_client, 180)
