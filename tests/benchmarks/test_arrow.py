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


# def test_contains(small_client):
#     """String contains"""
#     memory = cluster_memory(small_client)
#     df = timeseries_of_size(memory)
#     s = df.name.astype(pd.StringDtype("pyarrow")).persist()
#     result = s.str.contains("a")
#     wait(result, small_client, 10 * 60)
#
#
# def test_startswith(small_client):
#     """String starts with"""
#     memory = cluster_memory(small_client)
#     df = timeseries_of_size(memory)
#     s = df.name.astype(pd.StringDtype("pyarrow")).persist()
#     result = s.str.startswith("B")
#     wait(result, small_client, 10 * 60)
#
#
# def test_filter(small_client):
#     """How fast can we filter a DataFrame?"""
#     memory = cluster_memory(small_client)
#     df = timeseries_of_size(memory)
#     df.name = df.name.astype(pd.StringDtype("pyarrow"))
#     df = df.persist()
#     name = df.head(1).name.iloc[0]  # Get first name that appears
#     result = df[df.name == name]
#     wait(result, small_client, 10 * 60)
#
#
# def test_value_counts(small_client):
#     """Value counts on string values"""
#     memory = cluster_memory(small_client)
#     df = timeseries_of_size(memory)
#     s = df.name.astype(pd.StringDtype("pyarrow")).persist()
#     result = s.value_counts()
#     wait(result, small_client, 10 * 60)
