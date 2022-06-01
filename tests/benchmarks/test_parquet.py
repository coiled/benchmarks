"""
Parquet-related benchmarks.
"""
import uuid

import dask.dataframe as dd
import dask.datasets
import distributed
import fsspec
import pandas
import pytest
from coiled.v2 import Cluster


@pytest.fixture(scope="module")
def parquet_cluster():
    with Cluster(
        f"parquet-{uuid.uuid4().hex[:8]}",
        n_workers=15,
        worker_vm_types=["m5.xlarge"],
        scheduler_vm_types=["m5.xlarge"],
    ) as cluster:
        yield cluster


@pytest.fixture(scope="module")
def parquet_client(parquet_cluster):
    with distributed.Client(parquet_cluster) as client:
        client.restart()
        yield client


def test_read_spark_generated_data(parquet_client):
    """
    Read a ~15 GB subset of a ~800 GB spark-generated
    open dataset on AWS.
    """
    ddf = dd.read_parquet(
        "s3://coiled-runtime-ci/thousandgenomes_dragen/var_partby_samples/NA21**.parquet",
        engine="pyarrow",
        index="sample_id",
    )
    ddf.groupby(ddf.index).first().compute()


def test_read_hive_partitioned_data(parquet_client):
    """
    Read a dataset partitioned by year and quarter.
    """
    ddf = dd.read_parquet(
        "s3://coiled-runtime-ci/ookla-open-data/type=fixed/**.parquet",
        engine="pyarrow",
    )

    # The data is already partitioned by year and quarter, but it doesn't
    # fit Dask's partitioning scheme well since we don't support multiindexes.
    # This is a lot of work to set the index for something that is already
    # partitioned! We also go around dask's set_index, which does a huge amount
    # of extra work, and even takes down instances.
    def get_period(df):
        return df.set_index(
            pandas.PeriodIndex(
                df.year.astype(str) + "Q" + df.quarter.astype(str),
                freq="Q",
                name="period",
            )
        )

    ddf2 = ddf.map_partitions(get_period, meta=get_period(ddf._meta)).persist()
    distributed.wait(ddf2)
    # Enable once we have dask>=2022.3.0
    # ddf2.divisions = ddf2.compute_current_divisions()


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


@pytest.mark.parametrize("kind", ("s3fs", "pandas", "dask"))
def test_s3_ec2_throughput(parquet_client, kind):
    # Test throughput for downloading and parsing a ~500 MB file
    path = (
        "s3://coiled-runtime-ci/ookla-open-data/"
        "type=fixed/year=2022/quarter=1/2022-01-01_performance_fixed_tiles.parquet"
    )
    if kind == "s3fs":

        def load(path):
            with fsspec.open(path) as f:
                f.read()

        parquet_client.submit(load, path)
    elif kind == "pandas":
        parquet_client.submit(pandas.read_parquet, path, engine="pyarrow")
    elif kind == "dask":
        distributed.wait(dd.read_parquet(path, engine="pyarrow").persist())
