import os
import sys
import uuid

import coiled
import pytest
from dask.distributed import Client
from packaging.version import Version

# Use non-declarative clusters on Python 3.7 with `coiled <= 0.0.73`
# See https://github.com/coiled/coiled-runtime/pull/54
if tuple(sys.version_info[:2]) < (3, 8) and Version(coiled.__version__) <= Version(
    "0.0.73"
):
    from coiled import Cluster
else:
    from coiled._beta import ClusterBeta as Cluster


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
