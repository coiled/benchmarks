import contextlib
import datetime
import os
import time
import uuid
import warnings

import coiled
import dask
import filelock
import pytest
import requests
from dask.distributed import LocalCluster, performance_report

from .utils import get_dataset_path

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


@pytest.fixture(scope="function")
def benchmark_time(test_run_benchmark, module, scale, name):
    """Benchmark the wall clock time of executing some code.

    Yields
    ------
    Context manager that records the wall clock time duration of executing
    the ``with`` statement if run as part of a benchmark, or does nothing otherwise.

    Example
    -------
    .. code-block:: python

        def test_something(benchmark_time):
            with benchmark_time:
                do_something()
    """

    @contextlib.contextmanager
    def _benchmark_time():
        if not test_run_benchmark:
            yield
        else:
            start = time.time()
            yield
            end = time.time()
            test_run_benchmark.duration = end - start
            test_run_benchmark.start = datetime.datetime.utcfromtimestamp(start)
            test_run_benchmark.end = datetime.datetime.utcfromtimestamp(end)
            test_run_benchmark.cluster_name = f"tpch-{module}-{scale}-{name}"

    return _benchmark_time()


#############################################
# Multi-machine fixtures for Spark and Dask #
#############################################


@pytest.fixture(scope="session")
def cluster_spec(scale):
    everywhere = dict(
        idle_timeout="1h",
        wait_for_workers=True,
        scheduler_vm_types=["m6i.xlarge"],
    )
    if scale == 10:
        return {
            "worker_vm_types": ["m6i.large"],
            "n_workers": 8,
            **everywhere,
        }
    elif scale == 100:
        return {
            "worker_vm_types": ["m6i.large"],
            "n_workers": 16,
            **everywhere,
        }
    elif scale == 1000:
        return {
            "worker_vm_types": ["m6i.xlarge"],
            "n_workers": 32,
            **everywhere,
        }
    elif scale == 10000:
        return {
            "worker_vm_types": ["m6i.xlarge"],
            "n_workers": 32,
            "worker_disk_size": 200,
            **everywhere,
        }


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
        with dask.config.set({"distributed.scheduler.worker-saturation": "inf"}):
            with coiled.Cluster(**kwargs) as cluster:
                yield cluster


@pytest.fixture
def client(
    request,
    cluster,
    testrun_uid,
    cluster_kwargs,
    benchmark_time,
    restart,
    scale,
    local,
    query,
):
    with cluster.get_client() as client:
        if restart:
            client.restart()
        client.run(lambda: None)

        local = "local" if local else "cloud"
        if request.config.getoption("--performance-report"):
            if not os.path.exists("performance-reports"):
                os.mkdir("performance-reports")
            with performance_report(
                filename=os.path.join(
                    "performance-reports", f"{local}-{scale}-{query}.html"
                )
            ):
                with benchmark_time:
                    yield client
        else:
            with benchmark_time:
                yield client


@pytest.fixture(scope="module")
def spark_setup(cluster, local):
    pytest.importorskip("pyspark")

    if local:
        cluster.close()  # no need to bootstrap with Dask
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local[*]").getOrCreate()
        spark_dashboard, query = cluster.dashboard_link.split("?")
        spark_dashboard = f"{spark_dashboard.split(':')[0]}:4040?{query}"
    else:
        spark = cluster.get_spark()
        spark_dashboard = cluster.spark_dashboard

    spark._spark_dashboard = spark_dashboard

    # warm start
    from pyspark.sql import Row

    df = spark.createDataFrame(
        [
            Row(a=1, b=2.0, c="string1"),
        ]
    )
    df.show()

    yield spark


def get_number_spark_executors(spark_dashboard):
    # ie https://some.address.dask.host/spark?token=some-token
    #    http://127.0.0.1:4040?token=some-token
    base, query = spark_dashboard.split("?")
    base = base.removesuffix("/")
    query = f"?{query}" if query else ""

    url = f"{base}/api/v1/applications{query}"
    apps = requests.get(url).json()
    for app in apps:
        if app["name"] == "SparkConnectServer":
            appid = app["id"]
            break
    else:
        raise ValueError("Failed to find Spark application 'SparkConnectServer'")

    url = f"{base}/{appid}/allexecutors{query}"
    executors = requests.get(url).json()
    return sum(1 for executor in executors if executor["isActive"])


@pytest.fixture
def spark(spark_setup, benchmark_time):
    n_executors_start = get_number_spark_executors(spark_setup._spark_dashboard)
    with benchmark_time:
        yield spark_setup

    n_executors_finish = get_number_spark_executors(spark_setup._spark_dashboard)
    if n_executors_finish != n_executors_start:
        warnings.warn(
            "Executor count changed between start and end of yield. "
            f"Startd with {n_executors_start}, ended with {n_executors_finish}"
        )

    spark_setup.catalog.clearCache()


@pytest.fixture
def fs(local):
    if local:
        return None
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
    if scale == 10:
        return {
            "vm_type": "m6i.4xlarge",
        }
    elif scale == 100:
        return {
            "vm_type": "m6i.8xlarge",
        }
    elif scale == 1000:
        return {
            "vm_type": "m6i.32xlarge",
        }
    elif scale == 10000:
        return {
            "vm_type": "m6i.32xlarge",
            "disk_size": 1000,
        }


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
