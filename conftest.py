import os
import sys
import uuid

import pytest
import s3fs

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


S3_REGION = "us-east-2"


@pytest.fixture(scope="session")
def s3_bucket():
    return "dask-io"


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
def s3_scratch(s3, s3_bucket):
    # Ensure that the test-scratch directory exists,
    # but do NOT reomove it as multiple test runs could be
    # accessing it at the same time
    stability_url = f"{s3_bucket}/test-scratch"
    s3.mkdirs(stability_url, exist_ok=True)
    return stability_url


@pytest.fixture(scope="function")
def s3_url(s3, s3_scratch):
    urls = []

    def factory(prefix="test"):
        url = f"{s3_scratch}/{prefix}-{uuid.uuid4().hex}"
        s3.mkdirs(url)
        return f"s3://{url}"

    yield factory

    for url in urls:
        s3.rm(url, recursive=True)
