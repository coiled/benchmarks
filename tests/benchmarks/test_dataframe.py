from cgitb import small
from dask.sizeof import sizeof
from dask.utils import format_bytes
import dask.array as da
from dask.dataframe import from_pandas

import numpy as np
import pandas as pd

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


def test_dataframe_align(small_client):
    memory = cluster_memory(small_client)  # 76.66 GiB

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
    wait(final, small_client, 10 * 60)


def test_shuffle(small_client):
    memory = cluster_memory(small_client)  # 76.66 GiB

    df = timeseries_of_size(
        memory // 4,
        start="2020-01-01",
        freq="1200ms",
        partition_freq="24h",
        dtypes={i: float for i in range(100)},
    )
    print_dataframe_info(df)
    # ~25,488,000 rows x 100 columns, 19.18 GiB total, 354 55.48 MiB partitions

    shuf = df.shuffle(0, shuffle="tasks")
    result = shuf.size
    wait(result, small_client, 20 * 60)


def test_ddf_isin(small_client):
    # memory = cluster_memory(small_client)
    print(small_client.dashboard_link)
    rs = np.random.RandomState(42)
    n = 100_000_000
    a_column_unique_values = np.arange(1, n // 10)
    df = pd.DataFrame({"A": rs.choice(a_column_unique_values, n), "B": rs.random(n)})

    filter_values_list = sorted(
        rs.choice(a_column_unique_values, len(a_column_unique_values) // 2).tolist()
    )
    ddf = from_pandas(df, npartitions=8).persist()
    tmp_ddf = ddf[ddf["A"].isin(filter_values_list)].persist()
    wait(tmp_ddf, small_client, 20*60)