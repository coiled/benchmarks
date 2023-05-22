from collections import OrderedDict

import dask.dataframe as dd
import pytest

SCHEMA = OrderedDict(
    [
        ("GlobalEventID", "Int64"),
        ("Day", "Int64"),
        ("MonthYear", "Int64"),
        ("Year", "Int64"),
        ("FractionDate", "float64"),
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
        ("GoldsteinScale", "float64"),
        ("NumMentions", "Int64"),
        ("NumSources", "Int64"),
        ("NumArticles", "Int64"),
        ("AvgTone", "float64"),
        ("Actor1Geo_Type", "Int64"),
        ("Actor1Geo_Fullname", "string[pyarrow]"),
        ("Actor1Geo_CountryCode", "string[pyarrow]"),
        ("Actor1Geo_ADM1Code", "string[pyarrow]"),
        ("Actor1Geo_Lat", "float64"),
        ("Actor1Geo_Long", "float64"),
        ("Actor1Geo_FeatureID", "string[pyarrow]"),
        ("Actor2Geo_Type", "Int64"),
        ("Actor2Geo_Fullname", "string[pyarrow]"),
        ("Actor2Geo_CountryCode", "string[pyarrow]"),
        ("Actor2Geo_ADM1Code", "string[pyarrow]"),
        ("Actor2Geo_Lat", "float64"),
        ("Actor2Geo_Long", "float64"),
        ("Actor2Geo_FeatureID", "string[pyarrow]"),
        ("ActionGeo_Type", "Int64"),
        ("ActionGeo_Fullname", "string[pyarrow]"),
        ("ActionGeo_CountryCode", "string[pyarrow]"),
        ("ActionGeo_ADM1Code", "string[pyarrow]"),
        ("ActionGeo_Lat", "float64"),
        ("ActionGeo_Long", "float64"),
        ("ActionGeo_FeatureID", "string[pyarrow]"),
        ("DATEADDED", "Int64"),
        ("SOURCEURL", "string[pyarrow]"),
    ]
)


@pytest.mark.client("from_csv_to_parquet")
def test_from_csv_to_parquet(client, s3_factory, s3_url):
    s3 = s3_factory(anon=True)
    files = s3.ls("s3://gdelt-open-data/events/")[:1000]
    files = [f"s3://{f}" for f in files]

    df = dd.read_csv(
        files,
        sep="\t",
        names=SCHEMA.keys(),
        # 'dtype' and 'converters' cannot overlap
        dtype={col: dtype for col, dtype in SCHEMA.items() if dtype != "float64"},
        storage_options=s3.storage_options,
        on_bad_lines="skip",
        # Some bad files have '#' in float values
        converters={
            col: lambda v: float(v.replace("#", "") or "NaN")
            for col, dtype in SCHEMA.items()
            if dtype == "float64"
        },
    )

    # Now we can safely convert the float columns
    df = df.astype({col: dtype for col, dtype in SCHEMA.items() if dtype == "float64"})

    df = df.map_partitions(
        lambda xdf: xdf.drop_duplicates(subset=["SOURCEURL"], keep="first")
    )
    df["national_paper"] = df.SOURCEURL.str.contains(
        "washingtonpost|nytimes", regex=True
    )
    df = df[df["national_paper"]]
    df.to_parquet(f"{s3_url}/from-csv-to-parquet/", write_index=False)
