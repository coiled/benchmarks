import os
import uuid

import coiled
import dask
import pytest
from dask.distributed import LocalCluster

_scale = 100
_local = False


@pytest.fixture(scope="session")
def scale():
    return _scale


@pytest.fixture(scope="session")
def local():
    return _local  # TODO: this is ugly.  done for coiled_function


@pytest.fixture(scope="session")
def dataset_path(local, scale):
    remote_paths = {
        10: "s3://coiled-runtime-ci/tpch_scale_10/",
        100: "s3://coiled-runtime-ci/tpch_scale_100/",
        1000: "s3://coiled-runtime-ci/tpch-scale-1000-date/",
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


#################################################
# Single machine fixtures for Polars and DuckDB #
#################################################


machine = {  # TODO: figure out where to place this
    "vm_type": "m6i.8xlarge",
}

_uuid = uuid.uuid4().hex[:8]


def coiled_function():
    def _(function):
        if _local:
            return function
        else:
            module = function.__module__.split("_")[-1]  # polars, duckdb, ...
            return coiled.function(
                **machine,
                name=f"tpch-{module}-{_scale}-{_uuid}",
            )(function)

    return _


@pytest.fixture(scope="module")
def coiled_function_client(module, local):
    if not local:

        @coiled.function(
            **machine,
            name=f"tpch-{module}-{_scale}-{_uuid}",
        )
        def _():
            pass

        yield _.client

    else:
        yield None


@pytest.fixture(scope="module")
def warm_start(coiled_function_client):
    if not local:
        coiled_function_client.submit(lambda: 0).result()


@pytest.fixture(scope="function")
def restart(benchmark_time, warm_start, coiled_function_client):
    if not local:
        coiled_function_client.restart()

    with benchmark_time:
        yield
