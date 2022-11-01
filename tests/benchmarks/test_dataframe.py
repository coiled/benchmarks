import random
import string

import dask.dataframe as dd
import numpy
import pandas
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


@pytest.mark.parametrize("dtype", ["object", "string[python]", "string[pyarrow]"])
def test_shuffle_string(dtype, small_client):
    def make_partition(s, n=100_000):
        random.seed(s)
        s1 = pandas.Series(
            [
                "".join(
                    random.choices(string.ascii_letters, k=random.randint(100, 1000))
                )
                for _ in range(n)
            ],
            dtype=dtype,
            name="label",
        )
        df = pandas.DataFrame(numpy.random.randint(0, 100, size=(n, 10)))
        df.insert(0, "label", s1)
        return df

    meta = make_partition(0, n=10)

    ddf = dd.from_map(make_partition, range(100), meta=meta)
    result = ddf.set_index("label").persist()
    wait(result, small_client, 20 * 60)
