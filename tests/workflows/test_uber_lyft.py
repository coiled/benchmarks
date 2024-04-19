import dask_expr as dd
import pytest

pytestmark = pytest.mark.workflows


@pytest.mark.client("uber_lyft")
def test_exploratory_analysis(client):
    """Run some exploratory aggs on the dataset"""

    # NYC taxi Uber/Lyft dataset
    df = dd.read_parquet(
        "s3://coiled-datasets/uber-lyft-tlc/", storage_options={"anon": True}
    )

    # Preprocessing:
    #   - Add a column to indicate company, instead of license number
    #   - Add a column to indicate if a tip was given
    taxi_companies = {
        "HV0002": "Juno",
        "HV0003": "Uber",
        "HV0004": "Via",
        "HV0005": "Lyft",
    }
    df["company"] = df.hvfhs_license_num.replace(taxi_companies)
    df["tipped"] = df.tips > 0

    # Persist so we only read once
    df = df.persist()

    # How many riders tip?
    df.tipped.mean().compute()
    # How many riders tip for each company?
    df.groupby("company").tipped.value_counts().compute()
    # What are those as percentages?
    df.groupby("company").tipped.mean().compute()
