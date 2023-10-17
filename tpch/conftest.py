import functools
import os
import uuid

import coiled
import dask
import pytest
from dask.distributed import Client, LocalCluster


@pytest.fixture(scope="module")
def warm_start():
    if not local:

        @coiled.function(**machine)
        def _():
            pass

        _()  # run once to give us a warm start


@pytest.fixture(scope="function")
def restart(warm_start, benchmark_all, benchmark_time, local):
    if local:
        with benchmark_time:
            yield
    else:

        @coiled.function(**machine)
        def _():
            pass

        _.client.restart()
        with benchmark_all(_.client):
            yield


machine = {  # TODO: figure out where to place this
    "vm_type": "m6i.8xlarge",
}


def coiled_function(**kwargs):
    # Shouldn't be necessary
    # See https://github.com/coiled/platform/issues/3519
    def _(function):
        if _local:
            return function
        else:
            return functools.wraps(function)(
                coiled.function(**kwargs, **machine)(function)
            )

    return _


@pytest.fixture(scope="session")
def scale():
    return 100


_local = False


@pytest.fixture(scope="session")
def local():
    return _local  # TODO: this is ugly.  done for coiled_function


@pytest.fixture(scope="session")
def dataset_path(local, scale):
    remote_paths = {
        10: "s3://coiled-runtime-ci/tpch_scale_10/",
        100: "s3://coiled-runtime-ci/tpch_scale_100/",
        1000: "s3://coiled-runtime-ci/tpch-scale-1000/",
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


@pytest.fixture(scope="module")
def cluster(
    local, scale, module, dask_env_variables, cluster_kwargs, github_cluster_tags
):
    if local:
        with LocalCluster() as cluster:
            yield cluster
    else:
        kwargs = dict(
            name=f"{module}-{uuid.uuid4().hex[:8]}",
            environ=dask_env_variables,
            tags=github_cluster_tags,
            region="us-east-2",
            **cluster_kwargs["tpch"],
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
    benchmark_all,
):
    with cluster.get_client() as client:
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


@pytest.fixture(scope="module")
def pyspark_cluster(
    request, cluster, dask_env_variables, cluster_kwargs, github_cluster_tags
):
    from .test_pyspark import SparkMaster, SparkWorker

    with cluster.get_client() as client:
        client.register_plugin(SparkMaster(), name="spark-master")
        client.register_plugin(SparkWorker())

    yield cluster


@pytest.fixture
def pyspark_client(pyspark_cluster, benchmark_all):
    with Client(pyspark_cluster) as client:
        client.restart()
        client.run(lambda: None)

        with benchmark_all(client):
            yield client
