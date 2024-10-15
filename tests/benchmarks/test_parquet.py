"""
Parquet-related benchmarks.
"""
import io
import uuid

import boto3
import dask.dataframe as dd
import dask.datasets
import distributed
import fsspec
import pandas
import pytest
from coiled import Cluster
from packaging.version import Version

from ..conftest import dump_cluster_kwargs
from ..utils_test import run_up_to_nthreads, wait

try:
    import pyarrow

    HAS_PYARROW12 = Version(pyarrow.__version__) >= Version("12.0.0")
except ImportError:
    HAS_PYARROW12 = False


@pytest.fixture(scope="module")
def parquet_cluster(dask_env_variables, cluster_kwargs, github_cluster_tags):
    kwargs = dict(
        name=f"parquet-{uuid.uuid4().hex[:8]}",
        environ=dask_env_variables,
        tags=github_cluster_tags,
        **cluster_kwargs["parquet_cluster"],
    )
    dump_cluster_kwargs(kwargs, "parquet")

    with Cluster(**kwargs) as cluster:
        yield cluster


@pytest.fixture
def parquet_client(parquet_cluster, cluster_kwargs, benchmark_all):
    n_workers = cluster_kwargs["parquet_cluster"]["n_workers"]
    with distributed.Client(parquet_cluster) as client:
        parquet_cluster.scale(n_workers)
        client.wait_for_workers(n_workers, timeout=600)
        client.restart()
        with benchmark_all(client):
            yield client


@pytest.mark.skipif(
    HAS_PYARROW12,
    reason="50x slower than PyArrow 11; https://github.com/coiled/benchmarks/issues/998",
)
@run_up_to_nthreads("parquet_cluster", 100, reason="fixed dataset")
def test_read_spark_generated_data(parquet_client):
    """
    Read a ~15 GB subset of a ~800 GB spark-generated
    open dataset on AWS.

    The dataset was copied from AWS open data on 2022-05-25
    https://registry.opendata.aws/1000-genomes-data-lakehouse-ready/
    Citation: https://www.nature.com/articles/s41467-018-08148-z
    """
    ddf = dd.read_parquet(
        "s3://coiled-runtime-ci/thousandgenomes_dagen/NA21**.parquet",
        engine="pyarrow",
        index="sample_id",
    )
    coll = ddf.groupby(ddf.index).first()
    wait(coll, parquet_client, 500)


@run_up_to_nthreads("parquet_cluster", 100, reason="fixed dataset")
def test_read_hive_partitioned_data(parquet_client):
    """
    Read a dataset partitioned by year and quarter.

    The dataset was copied from AWS open data on 2022-05-25
    https://registry.opendata.aws/speedtest-global-performance/
    """
    ddf = dd.read_parquet(
        "s3://coiled-runtime-ci/ookla-open-data/type=fixed/*/*/*.parquet",
        engine="pyarrow",
    )
    coll = ddf.groupby(["year", "quarter"]).first()
    wait(coll, parquet_client, 100)


@run_up_to_nthreads("parquet_cluster", 100, reason="fixed dataset")
def test_write_wide_data(parquet_client, s3_url):
    # Write a ~700 partition, ~200 GB dataset with a lot of columns
    ddf = dask.datasets.timeseries(
        dtypes={
            **{f"name-{i}": str for i in range(25)},
            **{f"price-{i}": float for i in range(25)},
            **{f"id-{i}": int for i in range(25)},
            **{f"cat-{i}": "category" for i in range(25)},
        },
        start="2021-01-01",
        end="2021-02-01",
        freq="10ms",
        partition_freq="1H",
    )
    ddf.to_parquet(s3_url + "/wide-data/")


@run_up_to_nthreads("parquet_cluster", 60, reason="fixed dataset")
@pytest.mark.parametrize("kind", ["boto3", "s3fs", "pandas", "pandas+boto3", "dask"])
def test_download_throughput(parquet_client, kind):
    """Test throughput for downloading and parsing a single 563 MB parquet file.

    Note
    ----
    I/O performance on S3 is heavily dependent on how many times the same file has been
    requested over the last few seconds. In A/B tests, this could lead to a false
    impression that test cases later in this list are faster than the earlier ones.
    Read more: https://github.com/coiled/benchmarks/issues/821
    """
    path = (
        "s3://coiled-runtime-ci/ookla-open-data/"
        "type=fixed/year=2022/quarter=1/2022-01-01_performance_fixed_tiles.parquet"
    )

    def boto3_load(path):
        s3 = boto3.client("s3")
        _, _, bucket_name, key = path.split("/", maxsplit=3)
        response = s3.get_object(Bucket=bucket_name, Key=key)
        return response["Body"].read()

    if kind == "boto3":
        fut = parquet_client.submit(boto3_load, path)

    elif kind == "s3fs":

        def load(path):
            with fsspec.open(path) as f:
                return f.read()

        fut = parquet_client.submit(load, path)

    elif kind == "pandas":
        fut = parquet_client.submit(pandas.read_parquet, path, engine="pyarrow")

    elif kind == "pandas+boto3":

        def load(path):
            raw = boto3_load(path)
            buf = io.BytesIO(raw)
            return pandas.read_parquet(buf, engine="pyarrow")

        fut = parquet_client.submit(load, path)

    elif kind == "dask":
        fut = dd.read_parquet(path, engine="pyarrow")

    wait(fut, parquet_client, timeout=60)
