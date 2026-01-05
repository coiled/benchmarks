import pandas as pd
import pytest

from ..utils_test import cluster_memory, timeseries_of_size, wait


@pytest.fixture(params=[True, False], ids=["string[pyarrow]", "object"])
def series(request, small_client):
    memory = cluster_memory(small_client)
    df = timeseries_of_size(memory)
    series = df.name
    if request.param:
        series = series.astype(pd.StringDtype("pyarrow"))
    series = series.persist()
    yield series


def test_unique(series, small_client):
    """Find unique values"""
    result = series.unique()
    wait(result, small_client, 10 * 60)


def test_contains(series, small_client):
    """String contains"""
    result = series.str.contains("a")
    wait(result, small_client, 10 * 60)


def test_startswith(series, small_client):
    """String starts with"""
    result = series.str.startswith("B")
    wait(result, small_client, 10 * 60)


def test_upper(series, small_client):
    """String upper"""
    result = series.str.upper()
    wait(result, small_client, 10 * 60)


def test_filter(series, small_client):
    """How fast can we filter the Series"""
    name = series.head(1)[0]  # Get first name that appears
    result = series[series == name]
    wait(result, small_client, 10 * 60)


def test_value_counts(series, small_client):
    """Value counts on string values"""
    result = series.value_counts()
    wait(result, small_client, 10 * 60)
