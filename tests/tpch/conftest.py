import os
import uuid

import coiled
import dask
import pytest
from dask.distributed import LocalCluster

##################
# Global Options #
##################


def pytest_addoption(parser):
    parser.addoption("--local", action="store_true", default=False, help="")
    parser.addoption("--restart", action="store_true", default=True, help="")
    parser.addoption("--no-restart", action="store_false", dest="restart", help="")

    parser.addoption(
        "--scale",
        action="store",
        default=10,
        help="Scale to run, 10, 100, 1000, or 10000",
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
def dataset_path(local, scale):
    remote_paths = {
        10: "s3://coiled-runtime-ci/tpch/scale-10/",
        100: "s3://coiled-runtime-ci/tpch/scale-100/",
        1000: "s3://coiled-runtime-ci/tpch/scale-1000/",
        10000: "s3://coiled-runtime-ci/tpch/scale-10000/",
    }
    local_paths = {
        10: "./tpch-data/scale10/",
        100: "./tpch-data/scale100/",
    }

    if local:
        return local_paths[scale]
    else:
        return remote_paths[scale]


@pytest.fixture(scope="module")
def module(request):
    module = os.path.basename(request.fspath).split(".")[0]
    module = module.replace("test_", "")
    return module


#############################################
# Multi-machine fixtures for Spark and Dask #
#############################################


@pytest.fixture(scope="module")
def cluster(
    local, scale, module, dask_env_variables, cluster_kwargs, github_cluster_tags
):
    if local:
        with LocalCluster() as cluster:
            yield cluster
    else:
        kwargs = dict(
            name=f"tpch-{module}-{scale}-{uuid.uuid4().hex[:8]}",
            environ=dask_env_variables,
            tags=github_cluster_tags,
            region="us-east-2",
            **cluster_kwargs["tpch"],
        )
        with dask.config.set({"distributed.scheduler.worker-saturation": "inf"}):
            with coiled.Cluster(**kwargs) as cluster:
                yield cluster


@pytest.fixture
def client(request, cluster, testrun_uid, cluster_kwargs, benchmark_all, restart):
    with cluster.get_client() as client:
        if restart:
            client.restart()
        client.run(lambda: None)

        with benchmark_all(client):
            yield client


@pytest.fixture(scope="module")
def spark_setup(cluster, local):
    pytest.importorskip("pyspark")
    if local:
        cluster.close()  # no need to bootstrap with Dask
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local[*]").getOrCreate()
    else:
        from coiled.spark import get_spark

        with cluster.get_client() as client:
            spark = get_spark(client)

    # warm start
    from pyspark.sql import Row

    df = spark.createDataFrame(
        [
            Row(a=1, b=2.0, c="string1"),
        ]
    )
    df.show()

    yield spark


@pytest.fixture
def spark(spark_setup, benchmark_time):
    with benchmark_time:
        yield spark_setup

    spark_setup.catalog.clearCache()


#################################################
# Single machine fixtures for Polars and DuckDB #
#################################################


machine = {  # TODO: figure out where to place this
    "vm_type": "m6i.8xlarge",
}

_uuid = uuid.uuid4().hex[:8]


@pytest.fixture(scope="module")
def module_run(local, module, scale):
    if local:

        def _run(function):
            return function()

        yield _run

    else:

        @coiled.function(**machine, name=f"tpch-{module}-{scale}-{_uuid}")
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
def run(module_run, restart, benchmark_time, warm_start):
    if restart and not local:
        module_run.client.restart()

    with benchmark_time:
        yield module_run
