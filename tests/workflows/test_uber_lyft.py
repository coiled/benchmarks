import uuid

import coiled
import dask.dataframe as dd
import pytest
from dask.distributed import Client


@pytest.fixture(scope="module")
def uber_lyft_cluster(
    dask_env_variables,
    cluster_kwargs,
    github_cluster_tags,
):
    with coiled.Cluster(
        f"uber-lyft-{uuid.uuid4().hex[:8]}",
        environ=dask_env_variables,
        tags=github_cluster_tags,
        **cluster_kwargs["uber_lyft_cluster"],
    ) as cluster:
        yield cluster


@pytest.fixture
def uber_lyft_client(
    uber_lyft_cluster,
    cluster_kwargs,
    upload_cluster_dump,
    benchmark_all,
):
    n_workers = cluster_kwargs["uber_lyft_cluster"]["n_workers"]
    with Client(uber_lyft_cluster) as client:
        uber_lyft_cluster.scale(n_workers)
        client.wait_for_workers(n_workers)
        client.restart()
        with upload_cluster_dump(client), benchmark_all(client):
            yield client


def test_explore_dataset(uber_lyft_client):
    """Run some exploratory aggs on the dataset"""

    # NYC taxi Uber/Lyft dataset
    columns = [
        "hvfhs_license_num",
        "tips",
        "base_passenger_fare",
        "driver_pay",
        "trip_miles",
        "trip_time",
        "shared_request_flag",
    ]
    ddf = dd.read_parquet("s3://coiled-datasets/uber-lyft-tlc/", columns=columns)

    # convert object column to arrow string
    ddf["hvfhs_license_num"] = ddf["hvfhs_license_num"].astype("string[pyarrow]")

    taxi_companies = {
        "HV0002": "Juno",
        "HV0003": "Uber",
        "HV0004": "Via",
        "HV0005": "Lyft",
    }

    # add a column to indicate company, instead of license number
    ddf["company"] = ddf.hvfhs_license_num.replace(taxi_companies)

    # add a column to indicate that tip was given
    ddf["tip_flag"] = ddf.tips > 0

    # what percentage of fare is the tip
    ddf["tip_percentage"] = ddf.tips / ddf.base_passenger_fare

    # persist so we only read once
    ddf = ddf.persist()

    # how many riders tip, in general
    (ddf.tips != 0).mean().compute()

    ddf.groupby("company").tip_flag.value_counts().compute()

    # what's the largest and average pay and tips that driver got, by company
    ddf.groupby("company").agg(
        {"driver_pay": ["max", "mean"], "tips": ["max", "mean"]}
    ).compute()

    # tip percentage mean of trip with tip
    ddf.loc[lambda x: x.tip_flag].groupby("company").tip_percentage.mean().compute()

    # sample the dataset to filter outliers, result is a pandas dataframe
    df_tiny = (
        ddf.loc[lambda x: x.tip_flag][["trip_miles", "base_passenger_fare", "tips"]]
        .sample(frac=0.001)
        .compute()
    )

    # filter outliers by tips values
    tips_filter_vals = df_tiny.tips.quantile([0.25, 0.75]).values
    tips_condition = ddf.tips.between(*tips_filter_vals)

    # filter outliers by fare values
    fare_filter_vals = df_tiny.base_passenger_fare.quantile([0.25, 0.75]).values
    fares_condition = ddf.base_passenger_fare.between(*fare_filter_vals)

    # filter outliers by miles values
    miles_filter_vals = df_tiny.trip_miles.quantile([0.25, 0.75]).values
    miles_condition = ddf.trip_miles.between(*miles_filter_vals)

    # filter the dataset to remove outliers, and persist again
    ddf = ddf.loc[(tips_condition & fares_condition) & miles_condition].persist()

    # now compute tip stats again
    ddf.groupby("company").tip_percentage.mean().compute()

    # average trip time by provider
    ddf.groupby("hvfhs_license_num").trip_time.agg(
        ["min", "max", "mean", "std"]
    ).compute()

    # ride count per company
    ddf.groupby("company").count().compute()

    # how many passengers ride over 5 miles, per company
    def over_five(x):
        return x[x > 5].count() / x.count()

    ddf.groupby("company").trip_miles.apply(
        over_five, meta=("trip_miles", float)
    ).compute()
