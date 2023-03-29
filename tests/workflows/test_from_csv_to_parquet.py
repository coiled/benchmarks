import os
import uuid

import coiled
import dask.dataframe as dd
import pytest
from distributed import Client, LocalCluster, wait

LOCAL_WORKFLOW_RUN = os.environ.get("LOCAL_WORKFLOW_RUN")


@pytest.fixture(scope="module")
def from_csv_to_parquet_cluster(
    dask_env_variables,
    cluster_kwargs,
    github_cluster_tags,
):
    if LOCAL_WORKFLOW_RUN is not None:
        with LocalCluster() as cluster:
            yield cluster
    else:
        with coiled.Cluster(
            f"from-csv-to-parquet-{uuid.uuid4().hex[:8]}",
            environ=dask_env_variables,
            tags=github_cluster_tags,
            **cluster_kwargs["from_csv_to_parquet_cluster"],
        ) as cluster:
            yield cluster


@pytest.fixture
def from_csv_to_parquet_client(
    from_csv_to_parquet_cluster,
    cluster_kwargs,
    upload_cluster_dump,
    benchmark_all,
):
    if LOCAL_WORKFLOW_RUN is not None:
        with Client(from_csv_to_parquet_cluster) as client:
            yield client
    else:
        n_workers = cluster_kwargs["from_csv_to_parquet_cluster"]["n_workers"]
        with Client(from_csv_to_parquet_cluster) as client:
            from_csv_to_parquet_cluster.scale(n_workers)
            client.wait_for_workers(n_workers)
            client.restart()
            with upload_cluster_dump(client), benchmark_all(client):
                yield client


COLUMNSV1 = {
    "GlobalEventID": "Int64",
    "Day": "Int64",
    "MonthYear": "Int64",
    "Year": "Int64",
    "FractionDate": "Float64",
    "Actor1Code": "string[pyarrow]",
    "Actor1Name": "string[pyarrow]",
    "Actor1CountryCode": "string[pyarrow]",
    "Actor1KnownGroupCode": "string[pyarrow]",
    "Actor1EthnicCode": "string[pyarrow]",
    "Actor1Religion1Code": "string[pyarrow]",
    "Actor1Religion2Code": "string[pyarrow]",
    "Actor1Type1Code": "string[pyarrow]",
    "Actor1Type2Code": "string[pyarrow]",
    "Actor1Type3Code": "string[pyarrow]",
    "Actor2Code": "string[pyarrow]",
    "Actor2Name": "string[pyarrow]",
    "Actor2CountryCode": "string[pyarrow]",
    "Actor2KnownGroupCode": "string[pyarrow]",
    "Actor2EthnicCode": "string[pyarrow]",
    "Actor2Religion1Code": "string[pyarrow]",
    "Actor2Religion2Code": "string[pyarrow]",
    "Actor2Type1Code": "string[pyarrow]",
    "Actor2Type2Code": "string[pyarrow]",
    "Actor2Type3Code": "string[pyarrow]",
    "IsRootEvent": "Int64",
    "EventCode": "string[pyarrow]",
    "EventBaseCode": "string[pyarrow]",
    "EventRootCode": "string[pyarrow]",
    "QuadClass": "Int64",
    "GoldsteinScale": "Float64",
    "NumMentions": "Int64",
    "NumSources": "Int64",
    "NumArticles": "Int64",
    "AvgTone": "Float64",
    "Actor1Geo_Type": "Int64",
    "Actor1Geo_Fullname": "string[pyarrow]",
    "Actor1Geo_CountryCode": "string[pyarrow]",
    "Actor1Geo_ADM1Code": "string[pyarrow]",
    "Actor1Geo_Lat": "Float64",
    "Actor1Geo_Long": "Float64",
    "Actor1Geo_FeatureID": "string[pyarrow]",
    "Actor2Geo_Type": "Int64",
    "Actor2Geo_Fullname": "string[pyarrow]",
    "Actor2Geo_CountryCode": "string[pyarrow]",
    "Actor2Geo_ADM1Code": "string[pyarrow]",
    "Actor2Geo_Lat": "Float64",
    "Actor2Geo_Long": "Float64",
    "Actor2Geo_FeatureID": "string[pyarrow]",
    "ActionGeo_Type": "Int64",
    "ActionGeo_Fullname": "string[pyarrow]",
    "ActionGeo_CountryCode": "string[pyarrow]",
    "ActionGeo_ADM1Code": "string[pyarrow]",
    "ActionGeo_Lat": "Float64",
    "ActionGeo_Long": "Float64",
    "ActionGeo_FeatureID": "string[pyarrow]",
    "DATEADDED": "Int64",
    "SOURCEURL": "string[pyarrow]",
}


def drop_dupe_per_partition(df):
    return df.drop_duplicates(subset=["SOURCEURL"], keep="first")


def test_from_csv_to_parquet(from_csv_to_parquet_client, s3_factory, s3_url):
    s3 = s3_factory(anon=True)
    df = dd.read_csv(
        "s3://gdelt-open-data/events/*.csv",
        names=COLUMNSV1.keys(),
        sep="\t",
        dtype=COLUMNSV1,
        storage_options=s3.storage_options,
    )

    df = df.partitions[-10:]
    df = df.map_partitions(drop_dupe_per_partition)
    national_paper = df.SOURCEURL.str.contains("washingtonpost|nytimes", regex=True)
    df["national_paper"] = national_paper
    df = df[df["national_paper"]]

    if LOCAL_WORKFLOW_RUN:
        output = "test-output"
    else:
        output = s3_url + "/from-csv-to-parquet/"


    to_pq = df.to_parquet(output, compute=False)

    future = from_csv_to_parquet_client.compute(to_pq)
    wait(future)
    newdf = dd.read_parquet(output)
    assert "DATEADDED" in list(newdf.columns)
