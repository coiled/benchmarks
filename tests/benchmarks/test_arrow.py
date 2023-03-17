from ..utils_test import cluster_memory, timeseries_of_size, wait


def test_unique(small_client, convert_string):
    """Find unique values"""
    memory = cluster_memory(small_client)
    df = timeseries_of_size(memory)
    result = df.name.unique()
    wait(result, small_client, 10 * 60)


def test_contains(small_client, convert_string):
    """String contains"""
    memory = cluster_memory(small_client)
    df = timeseries_of_size(memory)
    result = df.name.str.contains("a")
    wait(result, small_client, 10 * 60)


def test_startswith(small_client, convert_string):
    """String starts with"""
    memory = cluster_memory(small_client)
    df = timeseries_of_size(memory)
    result = df.name.str.startswith("B")
    wait(result, small_client, 10 * 60)


def test_filter(small_client, convert_string):
    """How fast can we filter a DataFrame?"""
    memory = cluster_memory(small_client)
    df = timeseries_of_size(memory)
    name = df.head(1).name.iloc[0]  # Get first name that appears
    result = df[df.name == name]
    wait(result, small_client, 10 * 60)


def test_value_counts(small_client, convert_string):
    """Value counts on string values"""
    memory = cluster_memory(small_client)
    df = timeseries_of_size(memory)
    result = df.name.value_counts()
    wait(result, small_client, 10 * 60)
