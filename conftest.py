import os
import sys
import uuid

import pytest
from coiled._beta import ClusterBeta as Cluster
from dask.distributed import Client


def pytest_addoption(parser):
    parser.addoption(
        "--run-latest", action="store_true", help="Run latest coiled-runtime tests"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-latest"):
        # --run-latest given in cli: do not skip latest coiled-runtime tests
        return
    skip_latest = pytest.mark.skip(reason="need --run-latest option to run")
    for item in items:
        if "latest_runtime" in item.keywords:
            item.add_marker(skip_latest)


@pytest.fixture(scope="session")
def software():
    return os.environ.get(
        "COILED_SOFTWARE_NAME",
        f"dask-engineering/coiled_dist-py{sys.version_info[0]}{sys.version_info[1]}",
    )


@pytest.fixture(scope="module")
def small_cluster(software, request):
    module = os.path.basename(request.fspath).split(".")[0]
    with Cluster(
        name=f"{module}-{uuid.uuid4().hex[:8]}",
        software=software,
        account="dask-engineering",
        n_workers=10,
        worker_vm_types=["m5.large"],
        scheduler_vm_types=["m5.large"],
    ) as cluster:
        yield cluster


@pytest.fixture
def small_client(small_cluster):
    with Client(small_cluster) as client:
        small_cluster.scale(10)
        client.wait_for_workers(10)
        client.restart()
        yield client
