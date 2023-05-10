import os
import uuid
from collections import OrderedDict

import coiled
import dask.dataframe as dd
import pytest
from distributed import Client, LocalCluster, wait


@pytest.fixture(scope="module")
def from_csv_to_parquet_cluster(
    dask_env_variables,
    cluster_kwargs,
    github_cluster_tags,
):
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
    n_workers = cluster_kwargs["from_csv_to_parquet_cluster"]["n_workers"]
    with Client(from_csv_to_parquet_cluster) as client:
        from_csv_to_parquet_cluster.scale(n_workers)
        client.wait_for_workers(n_workers)
        client.restart()
        with upload_cluster_dump(client), benchmark_all(client):
            yield client


COLUMNSV1 = OrderedDict([
    ("GlobalEventID", "Int64"),
    ("Day", "Int64"),
    ("MonthYear", "Int64"),
    ("Year", "Int64"),
    ("FractionDate", "Float64"),
    ("Actor1Code", "string[pyarrow]"),
    ("Actor1Name", "string[pyarrow]"),
    ("Actor1CountryCode", "string[pyarrow]"),
    ("Actor1KnownGroupCode", "string[pyarrow]"),
    ("Actor1EthnicCode", "string[pyarrow]"),
    ("Actor1Religion1Code", "string[pyarrow]"),
    ("Actor1Religion2Code", "string[pyarrow]"),
    ("Actor1Type1Code", "string[pyarrow]"),
    ("Actor1Type2Code", "string[pyarrow]"),
    ("Actor1Type3Code", "string[pyarrow]"),
    ("Actor2Code", "string[pyarrow]"),
    ("Actor2Name", "string[pyarrow]"),
    ("Actor2CountryCode", "string[pyarrow]"),
    ("Actor2KnownGroupCode", "string[pyarrow]"),
    ("Actor2EthnicCode", "string[pyarrow]"),
    ("Actor2Religion1Code", "string[pyarrow]"),
    ("Actor2Religion2Code", "string[pyarrow]"),
    ("Actor2Type1Code", "string[pyarrow]"),
    ("Actor2Type2Code", "string[pyarrow]"),
    ("Actor2Type3Code", "string[pyarrow]"),
    ("IsRootEvent", "Int64"),
    ("EventCode", "string[pyarrow]"),
    ("EventBaseCode", "string[pyarrow]"),
    ("EventRootCode", "string[pyarrow]"),
    ("QuadClass", "Int64"),
    ("GoldsteinScale", "Float64"),
    ("NumMentions", "Int64"),
    ("NumSources", "Int64"),
    ("NumArticles", "Int64"),
    ("AvgTone", "Float64"),
    ("Actor1Geo_Type", "Int64"),
    ("Actor1Geo_Fullname", "string[pyarrow]"),
    ("Actor1Geo_CountryCode", "string[pyarrow]"),
    ("Actor1Geo_ADM1Code", "string[pyarrow]"),
    ("Actor1Geo_Lat", "Float64"),
    ("Actor1Geo_Long", "Float64"),
    ("Actor1Geo_FeatureID", "string[pyarrow]"),
    ("Actor2Geo_Type", "Int64"),
    ("Actor2Geo_Fullname", "string[pyarrow]"),
    ("Actor2Geo_CountryCode", "string[pyarrow]"),
    ("Actor2Geo_ADM1Code", "string[pyarrow]"),
    ("Actor2Geo_Lat", "Float64"),
    ("Actor2Geo_Long", "Float64"),
    ("Actor2Geo_FeatureID", "string[pyarrow]"),
    ("ActionGeo_Type", "Int64"),
    ("ActionGeo_Fullname", "string[pyarrow]"),
    ("ActionGeo_CountryCode", "string[pyarrow]"),
    ("ActionGeo_ADM1Code", "string[pyarrow]"),
    ("ActionGeo_Lat", "Float64"),
    ("ActionGeo_Long", "Float64"),
    ("ActionGeo_FeatureID", "string[pyarrow]"),
    ("DATEADDED", "Int64"),
    ("SOURCEURL", "string[pyarrow]"),
])


def test_from_csv_to_parquet(from_csv_to_parquet_client, s3_factory, s3_url):
    client = from_csv_to_parquet_client

    s3 = s3_factory(anon=True)
    bad_files = [
        "gdelt-open-data/events/20161004.export.csv",
        "gdelt-open-data/events/20170106.export.csv",
        "gdelt-open-data/events/20170422.export.csv",
        "gdelt-open-data/events/20170802.export.csv",
        "gdelt-open-data/events/20170920.export.csv",
        "gdelt-open-data/events/20171021.export.csv",
        "gdelt-open-data/events/20180415.export.csv",
        "gdelt-open-data/events/20180416.export.csv",
        "gdelt-open-data/events/20180613.export.csv",
        "gdelt-open-data/events/20180806.export.csv",
        "gdelt-open-data/events/20190217.export.csv",
        "gdelt-open-data/events/20190613.export.csv",
    ]
    files = s3.ls("s3://gdelt-open-data/events/")[:1000]
    files = [f"s3://{f}" for f in files if f not in bad_files]

    df = dd.read_csv(
        files,
        names=COLUMNSV1.keys(),
        sep="\t",
        dtype=COLUMNSV1,
        storage_options=s3.storage_options,
        on_bad_lines="skip",
    )

    df = df.map_partitions(
        lambda xdf: xdf.drop_duplicates(subset=["SOURCEURL"], keep="first")
    )
    df["national_paper"] = df.SOURCEURL.str.contains(
        "washingtonpost|nytimes", regex=True
    )
    df = df[df["national_paper"]]
    df = df.persist()
    assert len(df)
    df.to_parquet(f"{s3_url}/from-csv-to-parquet/")
