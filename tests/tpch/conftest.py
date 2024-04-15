import contextlib
import os
import re
import sys
import uuid
from contextlib import nullcontext

import coiled
import dask
import dask_expr
import distributed
import filelock
import pytest
from dask.distributed import LocalCluster
from dask.distributed import performance_report as performance_report_func
from distributed.diagnostics.plugin import WorkerPlugin

from benchmark_schema import TPCHRun

from ..conftest import TEST_DIR, WORKFLOW_URL
from .utils import get_cluster_spec, get_dataset_path, get_single_vm_spec

##################
# Global Options #
##################


def pytest_addoption(parser):
    parser.addoption("--local", action="store_true", default=False, help="")
    parser.addoption("--cloud", action="store_false", dest="local", help="")
    parser.addoption("--restart", action="store_true", default=True, help="")
    parser.addoption("--no-restart", action="store_false", dest="restart", help="")
    parser.addoption(
        "--performance-report", action="store_true", default=False, help=""
    )
    parser.addoption(
        "--scale",
        action="store",
        default=10,
        help="Scale to run, 10, 100, 1000, or 10000",
    )
    parser.addoption(
        "--name",
        action="store",
        default="",
        help="Name to use for run",
    )
    parser.addoption("--plot", action="store_true", default=False, help="")
    parser.addoption(
        "--ignore-spark-executor-count",
        action="store_true",
        default=False,
        help="Don't raise an error on changes in number of Spark executors between `spark` fixture use.",
    )
    parser.addoption(
        "--no-shutdown",
        action="store_false",
        dest="shutdown_on_close",
        default=True,
        help="Don't shutdown cluster when test ends",
    )


@pytest.fixture(scope="session")
def scale(request):
    return int(request.config.getoption("scale"))


@pytest.fixture(scope="session")
def local(request):
    return request.config.getoption("local")


@pytest.fixture(scope="session")
def restart(request):
    return request.config.getoption("restart")


@pytest.fixture(scope="session")
def shutdown_on_close(request):
    return request.config.getoption("shutdown_on_close")


@pytest.fixture(scope="session")
def dataset_path(local, scale):
    return get_dataset_path(local, scale)


@pytest.fixture(scope="module")
def module(request):
    module = os.path.basename(request.fspath).split(".")[0]
    module = module.replace("test_", "")
    return module


@pytest.fixture(scope="function")
def query(request):
    pattern = re.compile(r"test_query_(\d+)")
    if re_match := pattern.match(request.node.name):
        return int(re_match.group(1))
    return None


@pytest.fixture(scope="session")
def name(request, tmp_path_factory, worker_id):
    if request.config.getoption("name"):
        return request.config.getoption("name")

    if worker_id == "master":
        # not executing in with multiple workers
        return uuid.uuid4().hex[:8]

    # get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    fn = root_tmp_dir / "data"
    with filelock.FileLock(str(fn) + ".lock"):
        if fn.is_file():
            data = fn.read_text()
        else:
            data = uuid.uuid4().hex[:8]
            fn.write_text(data)
    return data


@pytest.fixture
def performance_report(request, local, scale, query):
    if request.config.getoption("--performance-report"):
        os.makedirs("performance-reports", exist_ok=True)
        local = "local" if local else "cloud"
        return performance_report_func(
            filename=os.path.join(
                "performance-reports", f"{local}-{scale}-{query}.html"
            )
        )
    else:
        return nullcontext()


@pytest.fixture()
def database_table_schema(request, testrun_uid, scale, query, local):
    return TPCHRun(
        session_id=testrun_uid,
        name=request.node.name,
        originalname=request.node.originalname,
        path=str(request.node.path.relative_to(TEST_DIR)),
        dask_version=dask.__version__,
        dask_expr_version=dask_expr.__version__,
        distributed_version=distributed.__version__,
        python_version=".".join(map(str, sys.version_info)),
        platform=sys.platform,
        ci_run_url=WORKFLOW_URL,
        scale=scale,
        query=query,
        local=local,
    )


@pytest.fixture(scope="function")
def benchmark_all(
    benchmark_memory,
    get_cluster_info,
    benchmark_time,
):
    """Benchmark all available metrics and extracts cluster information

    Yields
    ------
    Context manager factory function which takes a ``distributed.Client``
    as input. The context manager records records all available metrics while
    executing the ``with`` statement if run as part of a benchmark,
    or does nothing otherwise.

    Example
    -------
    .. code-block:: python

        def test_something(benchmark_all):
            with benchmark_all(client):
                do_something()

    See Also
    --------
    benchmark_memory
    benchmark_task_durations
    benchmark_time
    get_cluster_info
    """

    @contextlib.contextmanager
    def _benchmark_all(client):
        if client:
            with (
                benchmark_memory(client),
                get_cluster_info(client.cluster),
                benchmark_time,
            ):
                yield
        else:
            with benchmark_time:
                yield

    yield _benchmark_all


#############################################
# Multi-machine fixtures for Spark and Dask #
#############################################


@pytest.fixture(scope="session")
def cluster_spec(scale, shutdown_on_close):
    return get_cluster_spec(scale=scale, shutdown_on_close=shutdown_on_close)


@pytest.fixture(scope="module")
def cluster(
    local,
    scale,
    module,
    dask_env_variables,
    cluster_spec,
    github_cluster_tags,
    name,
    make_chart,
):
    with dask.config.set({"distributed.scheduler.worker-saturation": "inf"}):
        if local:
            with LocalCluster() as cluster:
                yield cluster
        else:
            kwargs = dict(
                name=f"tpch-{module}-{scale}-{name}",
                environ=dask_env_variables,
                tags=github_cluster_tags,
                region="us-east-2",
                **cluster_spec,
            )
            with coiled.Cluster(**kwargs) as cluster:
                yield cluster


class TurnOnPandasCOW(WorkerPlugin):
    idempotent = True

    def setup(self, worker):
        import pandas as pd

        pd.set_option("mode.copy_on_write", True)


def _assert_no_queuing(dask_scheduler):
    import math

    assert dask_scheduler.WORKER_SATURATION == math.inf


@pytest.fixture()
def client(cluster, restart, benchmark_all):
    with cluster.get_client() as client:
        client.run_on_scheduler(_assert_no_queuing)
        if restart:
            client.restart()
        client.run(lambda: None)
        client.register_plugin(TurnOnPandasCOW(), name="enable-cow")
        with benchmark_all(client):
            yield client


@pytest.fixture
def fs(local):
    if local:
        return "pyarrow"
    else:
        import boto3
        from pyarrow.fs import S3FileSystem

        session = boto3.session.Session()
        credentials = session.get_credentials()

        fs = S3FileSystem(
            secret_key=credentials.secret_key,
            access_key=credentials.access_key,
            region="us-east-2",
            session_token=credentials.token,
        )
        return fs


#################################################
# Single machine fixtures for Polars and DuckDB #
#################################################


@pytest.fixture(scope="session")
def machine_spec(scale):
    return get_single_vm_spec(scale)


@pytest.fixture(scope="module")
def module_run(local, module, scale, name, machine_spec):
    if local:

        def _run(function):
            return function()

        yield _run

    else:

        @coiled.function(**machine_spec, name=f"tpch-{module}-{scale}-{name}")
        def _run(function):
            return function()

        yield _run


@pytest.fixture(scope="module")
def warm_start(module_run, local):
    module_run(lambda: None)

    yield

    if not local:
        module_run.cluster.shutdown()


@pytest.fixture(scope="function")
def run(module_run, local, restart, benchmark_all, warm_start, make_chart):
    client = None
    if restart and not local:
        client = module_run.client
        client.restart()

    with benchmark_all(client):
        yield module_run


#############
# Charting #
#############


@pytest.fixture(scope="session")
def make_chart(request, name, tmp_path_factory, local, scale):
    if not request.config.getoption("--benchmark"):
        # Won't create the sqlite DB, and thus won't be able
        # to read test run information
        yield
        return

    if not request.config.getoption("--plot"):
        # Don't generate the plot
        yield
        return

    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    lock = filelock.FileLock(root_tmp_dir / "tpch.lock")

    local = "local" if local else "cloud"

    try:
        yield
    finally:
        from .generate_plot import generate

        with lock:
            if not os.path.exists("charts"):
                os.mkdir("charts")
            generate(
                outfile=os.path.join("charts", f"{local}-{scale}-query-{name}.json"),
                name=name,
                scale=scale,
            )
