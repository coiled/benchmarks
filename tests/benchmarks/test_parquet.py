"""
Parquet-related benchmarks.
"""
import uuid

import dask.dataframe as dd
import dask.datasets
import distributed
import pandas
import pytest
from coiled import Cluster

from ..utils_test import run_up_to_nthreads


@pytest.fixture(scope="module")
def parquet_cluster(dask_env_variables, cluster_kwargs, github_cluster_tags):
    with Cluster(
        f"parquet-{uuid.uuid4().hex[:8]}",
        environ=dask_env_variables,
        tags=github_cluster_tags,
        **cluster_kwargs["parquet_cluster"],
    ) as cluster:
        yield cluster


@pytest.fixture
def parquet_client(parquet_cluster, cluster_kwargs, upload_cluster_dump, benchmark_all):
    n_workers = cluster_kwargs["parquet_cluster"]["n_workers"]
    with distributed.Client(parquet_cluster) as client:
        parquet_cluster.scale(n_workers)
        client.wait_for_workers(n_workers)
        client.restart()
        with upload_cluster_dump(client), benchmark_all(client):
            yield client


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
    ddf.groupby(ddf.index).first().compute()


@run_up_to_nthreads("parquet_cluster", 100, reason="fixed dataset")
def test_read_hive_partitioned_data(parquet_client):
    """
    Read a dataset partitioned by year and quarter.

    The dataset was copied from AWS open data on 2022-05-25
    https://registry.opendata.aws/speedtest-global-performance/
    """
    ddf = dd.read_parquet(
        "s3://coiled-runtime-ci/ookla-open-data/type=fixed/**.parquet",
        engine="pyarrow",
    )

    ddf.groupby(["year", "quarter"]).first().compute()


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


@run_up_to_nthreads("parquet_cluster", 100, reason="fixed dataset")
@pytest.mark.parametrize("kind", ("s3fs", "pandas", "dask"))
def test_download_throughput(parquet_client, kind):
    fsspec = pytest.importorskip("fsspec")

    # Test throughput for downloading and parsing a ~500 MB file
    path = (
        "s3://coiled-runtime-ci/ookla-open-data/"
        "type=fixed/year=2022/quarter=1/2022-01-01_performance_fixed_tiles.parquet"
    )
    if kind == "s3fs":

        def load(path):
            with fsspec.open(path) as f:
                f.read()

        distributed.wait(parquet_client.submit(load, path))
    elif kind == "pandas":
        distributed.wait(
            parquet_client.submit(pandas.read_parquet, path, engine="pyarrow")
        )
    elif kind == "dask":
        distributed.wait(dd.read_parquet(path, engine="pyarrow").persist())
