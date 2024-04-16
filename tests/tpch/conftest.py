import os
import uuid
import warnings
from contextlib import nullcontext

import coiled
import dask
import filelock
import pytest
import requests
from dask.distributed import LocalCluster
from dask.distributed import performance_report as performance_report_func
from distributed.diagnostics.plugin import WorkerPlugin
from urllib3.util import Url, parse_url

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
    return 1000


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
    if request.node.name.startswith("test_query_"):
        return int(request.node.name.split("_")[-1])
    else:
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


@pytest.fixture
def client(
    request,
    cluster,
    cluster_kwargs,
    get_cluster_info,
    performance_report,
    benchmark_time,
    span,
    restart,
):
    with cluster.get_client() as client:
        if restart:
            client.restart()
        client.run(lambda: None)

        client.register_plugin(TurnOnPandasCOW(), name="enable-cow")
        with get_cluster_info(cluster), performance_report, benchmark_time:
            yield client


@pytest.fixture(scope="module")
def spark_setup(cluster, local):
    pytest.importorskip("pyspark")

    spark_dashboard: Url
    if local:
        cluster.close()
        from pyspark.sql import SparkSession

        # Set app name to match that used in Coiled Spark
        spark = (
            SparkSession.builder.appName("SparkConnectServer")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            # By default, the driver is only allowed to use up to 1g of memory
            # which causes it to crash when collecting results
            .config("spark.driver.memory", "10g")
            .getOrCreate()
        )
        spark_dashboard = parse_url("http://localhost:4040")
    else:
        spark = cluster.get_spark(executor_memory_factor=0.8, worker_memory_factor=0.9)
        # Available on coiled>=1.12.4
        if not hasattr(cluster, "_spark_dashboard"):
            cluster._spark_dashboard = (
                parse_url(cluster._dashboard_address)._replace(path="/spark").url
            )
        spark_dashboard = parse_url(cluster._spark_dashboard)

    try:
        spark._spark_dashboard: Url = spark_dashboard

        # warm start
        from pyspark.sql import Row

        df = spark.createDataFrame(
            [
                Row(a=1, b=2.0, c="string1"),
            ]
        )
        df.show()
        yield spark
    finally:
        spark.stop()


def get_number_spark_executors(spark_dashboard: Url):
    base_path = spark_dashboard.path or ""

    url = spark_dashboard._replace(path=f"{base_path}/api/v1/applications")
    apps = requests.get(url.url).json()
    for app in apps:
        if app["name"] == "SparkConnectServer":
            appid = app["id"]
            break
    else:
        raise ValueError("Failed to find Spark application 'SparkConnectServer'")

    url = url._replace(path=f"{url.path}/{appid}/allexecutors")
    executors = requests.get(url.url).json()
    return sum(1 for executor in executors if executor["isActive"])


@pytest.fixture
def spark(request, spark_setup, benchmark_time):
    n_executors_start = get_number_spark_executors(spark_setup._spark_dashboard)
    with benchmark_time:
        yield spark_setup
    n_executors_finish = get_number_spark_executors(spark_setup._spark_dashboard)

    if n_executors_finish != n_executors_start:
        msg = (
            "Executor count changed between start and end of yield. "
            f"Startd with {n_executors_start}, ended with {n_executors_finish}"
        )
        if request.config.getoption("ignore-spark-executor-count"):
            warnings.warn(msg)
        else:
            raise RuntimeError(msg)

    spark_setup.catalog.clearCache()


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
def run(module_run, local, restart, benchmark_time, warm_start, make_chart):
    if restart and not local:
        module_run.client.restart()

    with benchmark_time:
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
