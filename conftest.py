import contextlib
import json
import logging
import os
import shlex
import subprocess
import sys
import threading
import uuid
from distutils.util import strtobool

import dask
import pytest
import s3fs
from dask.distributed import Client
from toolz import merge

try:
    from coiled.v2 import Cluster
except ImportError:
    from coiled._beta import ClusterBeta as Cluster


logger = logging.getLogger("coiled-runtime")
logger.setLevel(logging.INFO)

# So coiled logs can be displayed on test failure
logging.getLogger("coiled").setLevel(logging.INFO)


def pytest_addoption(parser):
    # Workaround for https://github.com/pytest-dev/pytest-xdist/issues/620
    if threading.current_thread() is not threading.main_thread():
        os._exit(1)

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
            return f"coiled/coiled-runtime-{version}-py{py_version}"
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
    # Extract `backend_options` for cluster from `backend_options` markers
    backend_options = merge(
        m.kwargs for m in request.node.iter_markers(name="backend_options")
    )
    module = os.path.basename(request.fspath).split(".")[0]
    with Cluster(
        name=f"{module}-{uuid.uuid4().hex[:8]}",
        n_workers=10,
        worker_vm_types=["t3.large"],
        scheduler_vm_types=["t3.large"],
        backend_options=backend_options,
    ) as cluster:
        yield cluster


@pytest.fixture
def small_client(small_cluster, upload_cluster_dump):
    with Client(small_cluster) as client:
        small_cluster.scale(10)
        client.wait_for_workers(10)
        client.restart()

        with upload_cluster_dump(client, small_cluster):
            yield client


S3_REGION = "us-east-2"
S3_BUCKET = "s3://coiled-runtime-ci"


@pytest.fixture(scope="session")
def s3_storage_options():
    return {
        "config_kwargs": {"region_name": S3_REGION},
        "key": os.environ["AWS_ACCESS_KEY_ID"],
        "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
    }


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
    s3.mkdirs(url, exist_ok=False)
    yield url
    s3.rm(url, recursive=True)


@pytest.fixture(scope="session")
def s3_cluster_dump_url(s3, s3_scratch):
    dump_url = f"{s3_scratch}/cluster_dumps"
    s3.mkdirs(dump_url, exist_ok=True)
    return dump_url


# this code was taken from pytest docs
# https://docs.pytest.org/en/latest/example/simple.html#making-test-result-information-available-in-fixtures
@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    # set a report attribute for each phase of a call, which can
    # be "setup", "call", "teardown"

    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture
def upload_cluster_dump(request, s3_cluster_dump_url, s3_storage_options):
    @contextlib.contextmanager
    def _upload_cluster_dump(client, cluster):
        failed = False
        # the code below is a workaround to make cluster dumps work with clients in fixtures
        # and outside fixtures.
        try:
            yield
        except:  # noqa: E722
            failed = True
            raise
        finally:
            cluster_dump = strtobool(os.environ.get("CLUSTER_DUMP", "false"))
            if cluster_dump and (failed or request.node.rep_call.failed):
                dump_path = f"{s3_cluster_dump_url}/{cluster.name}/{request.node.name}"
                logger.error(f"Cluster state dump can be found at: {dump_path}")
                client.dump_cluster_state(dump_path, **s3_storage_options)

    yield _upload_cluster_dump
