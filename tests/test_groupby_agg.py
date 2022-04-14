import os
import sys
import uuid

import dask.dataframe as dd
import pytest
from coiled._beta import ClusterBeta as Cluster
from dask.distributed import Client

SOFTWARE = os.environ.get(
    "COILED_SOFTWARE_NAME",
    f"dask-engineering/coiled_dist-py{sys.version_info[0]}{sys.version_info[1]}",
)


@pytest.mark.parametrize("partition_size", ["10MB", "100MB", "1GB"])
def test_quickstart(partition_size):
    with Cluster(
        software=SOFTWARE,
        name="test_groupby_agg" + str(uuid.uuid4()),
        account="dask-engineering",
        n_workers=10,
    ) as cluster:

        with Client(cluster) as client:  # noqa F841
            ddf = (
                dd.read_parquet(
                    "s3://coiled-datasets/nyc-tlc-with-metadata/2019",
                    storage_options={"anon": True},
                )
                .repartition(partition_size=partition_size)
                .set_index("DOLocationID")
            )
            ddf = ddf.assign(
                travel_time=ddf["tpep_dropoff_datetime"] - ddf["tpep_pickup_datetime"]
            )
            ddf2 = ddf.groupby(
                [
                    ddf["tpep_pickup_datetime"].dt.year,
                    ddf["tpep_pickup_datetime"].dt.month,
                    ddf["tpep_pickup_datetime"].dt.day,
                    ddf["tpep_pickup_datetime"].dt.hour,
                ],
            )[["passenger_count", "PULocationID", "total_amount"]].agg(
                {
                    "passenger_count": ["sum", "std"],
                    "PULocationID": ["count"],
                    "total_amount": ["min"],
                    "travel_time": ["mean"],
                }
            )
            ddf2.columns = ddf2.columns.droplevel(0)
            mem = ddf2.memory_usage(deep=True).sum().compute()

            assert hasattr(ddf2, "dask")
            assert mem > 0
