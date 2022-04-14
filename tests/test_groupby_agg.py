
import dask.dataframe as dd
import pytest


@pytest.mark.parametrize("partition_size", ["100MB"])
def test_groupby_agg(partition_size, small_client):
    # with Cluster(
    #     software=SOFTWARE,
    #     name="test_groupby_agg" + str(uuid.uuid4()),
    #     account="dask-engineering",
    #     n_workers=10,
    # ) as cluster:

        # with Client(cluster) as client:  # noqa F841
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
