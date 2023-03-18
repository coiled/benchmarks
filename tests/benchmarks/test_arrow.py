import pandas as pd
import pytest

from ..utils_test import cluster_memory, timeseries_of_size, wait


@pytest.fixture(params=[True, False])
def series_with_client(request, small_client):
    memory = cluster_memory(small_client)
    df = timeseries_of_size(memory)
    series = df.name
    if request.param:
        series = series.astype(pd.StringDtype("pyarrow"))
    series = series.persist()
    yield series, small_client


def test_unique(series_with_client):
    """Find unique values"""
    series, client = series_with_client
    result = series.unique()
    wait(result, client, 10 * 60)


def test_contains(series_with_client):
    """String contains"""
    series, client = series_with_client
    result = series.str.contains("a")
    wait(result, client, 10 * 60)


def test_startswith(series_with_client):
    """String starts with"""
    series, client = series_with_client
    result = series.str.startswith("B")
    wait(result, client, 10 * 60)


def test_filter(series_with_client):
    """How fast can we filter the Series"""
    series, client = series_with_client
    name = series.iloc[0]  # Get first name that appears
    result = series[series == name]
    wait(result, client, 10 * 60)


def test_value_counts(series_with_client):
    """Value counts on string values"""
    series, client = series_with_client
    result = series.value_counts()
    wait(result, client, 10 * 60)
