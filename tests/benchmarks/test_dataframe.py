import pytest
from dask.sizeof import sizeof
from dask.utils import format_bytes

from ..utils_test import cluster_memory, timeseries_of_size, wait


def print_dataframe_info(df):
    p = df.partitions[0].compute(scheduler="threads")
    partition_size = sizeof(p)
    total_size = partition_size * df.npartitions
    print(
        f"~{len(p) * df.npartitions:,} rows x {len(df.columns)} columns, "
        f"{format_bytes(total_size)} total, "
        f"{df.npartitions:,} {format_bytes(partition_size)} partitions"
    )


@pytest.mark.client("small")
def test_dataframe_align(client):
    memory = cluster_memory(client)  # 76.66 GiB

    df = timeseries_of_size(
        memory // 2,
        start="2020-01-01",
        freq="600ms",
        partition_freq="12h",
        dtypes={i: float for i in range(100)},
    )
    print_dataframe_info(df)
    # ~50,904,000 rows x 100 columns, 38.31 GiB total, 707 55.48 MiB partitions

    df2 = timeseries_of_size(
        memory // 4,
        start="2010-01-01",
        freq="600ms",
        partition_freq="12h",
        dtypes={i: float for i in range(100)},
    )
    print_dataframe_info(df2)
    # ~25,488,000 rows x 100 columns, 19.18 GiB total, 354 55.48 MiB partitions

    final = (df2 - df).mean()  # will be all NaN, just forcing alignment
    wait(final, client, 10 * 60)


@pytest.mark.client("small")
def test_shuffle(client, configure_shuffling, memory_multiplier):
    memory = cluster_memory(client)  # 76.66 GiB

    df = timeseries_of_size(
        memory * memory_multiplier,
        start="2020-01-01",
        freq="1200ms",
        partition_freq="24h",
        dtypes={str(i): float for i in range(100)},
    )
    print_dataframe_info(df)
    # ~25,488,000 rows x 100 columns, 19.18 GiB total, 354 55.48 MiB partitions

    shuf = df.shuffle("0").map_partitions(lambda x: x)
    result = shuf.size
    wait(result, client, 20 * 60)


@pytest.mark.client("small")
def test_filter(client):
    """How fast can we filter a DataFrame?"""
    memory = cluster_memory(client)
    df = timeseries_of_size(memory)
    name = df.head(1).name.iloc[0]  # Get first name that appears
    result = df[df.name == name]
    wait(result, client, 10 * 60)


@pytest.mark.client("small")
def test_dataframe_cow_chain(client):
    memory = cluster_memory(client)  # 76.66 GiB

    df = timeseries_of_size(
        memory // 2,
        start="2020-01-01",
        freq="600ms",
        partition_freq="12h",
        dtypes={
            **{i: float for i in range(40)},
            **{i: int for i in range(41, 80)},
            **{i: object for i in range(81, 120)},
        },
    )
    print_dataframe_info(df)

    result = (
        df.rename(columns={1: 1000})
        .replace("x", "xxx")
        .fillna({i: 100 for i in range(10, 70)})
        .astype({50: "float"})
        .loc[:, slice(2, 100)]
    )
    wait(result, client, 10 * 60)
