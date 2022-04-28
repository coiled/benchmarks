import json
import logging
import os
import shlex
import subprocess
import sys
import uuid

import dask
import pytest
import s3fs
from dask.distributed import Client

try:
    from coiled.v2 import Cluster
except ImportError:
    from coiled._beta import ClusterBeta as Cluster


# So coiled logs can be displayed on test failure
logging.getLogger("coiled").setLevel(logging.INFO)


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


def get_software():
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


dask.config.set(
    {
        "coiled.account": "dask-engineering",
        "coiled.software": get_software(),
    }
)


@pytest.fixture(scope="module")
def small_cluster(request):
    module = os.path.basename(request.fspath).split(".")[0]
    with Cluster(
        name=f"{module}-{uuid.uuid4().hex[:8]}",
        n_workers=10,
        worker_memory="8 GiB",
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


S3_REGION = "us-east-2"
S3_BUCKET = "s3://dask-io"


@pytest.fixture(scope="session")
def s3_storage_options():
    return {"config_kwargs": {"region_name": S3_REGION}}


@pytest.fixture(scope="session")
def s3():
    return s3fs.S3FileSystem(
        key=os.environ["AWS_ACCESS_KEY_ID"],
        secret=os.environ["AWS_SECRET_ACCESS_KEY"],
        client_kwargs={"region_name": S3_REGION},
    )


@pytest.fixture(scope="session")
def s3_scratch(s3):
    # Ensure that the test-scratch directory exists,
    # but do NOT remove it as multiple test runs could be
    # accessing it at the same time
    scratch_url = f"{S3_BUCKET}/test-scratch"
    s3.mkdirs(scratch_url, exist_ok=True)
    return scratch_url


@pytest.fixture(scope="function")
def s3_url(s3, s3_scratch, request):
    url = f"{s3_scratch}/{request.node.originalname}-{uuid.uuid4().hex}"
    print(f"Creating {url}")
    s3.mkdirs(url, exist_ok=False)
    yield url
    print(f"Removing {url}")
    s3.rm(url, recursive=True)
