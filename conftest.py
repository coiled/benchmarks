import json
import os
import shlex
import subprocess
import sys
import uuid

import pytest

try:
    from coiled.v2 import Cluster
except ImportError:
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
    try:
        return os.environ["COILED_SOFTWARE_NAME"]
    except KeyError:
        # Determine software environment from local `coiled-runtime` version (in installed)
        out = subprocess.check_output(
            shlex.split("conda list --json coiled-runtime"), text=True
        ).rstrip()
        runtime_info = json.loads(out)
        if runtime_info:
            version = runtime_info[0]["version"].replace(".", "-")
            py_version = f"{sys.version_info[0]}{sys.version_info[1]}"
            return f"dask-engineering/coiled-runtime-{version}-py{py_version}"
        else:
            raise RuntimeError(
                "Must either specific `COILED_SOFTWARE_NAME` environment variable "
                "or have `coiled-runtime` installed"
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
