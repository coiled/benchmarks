import pytest
import numpy as np
import dask
from dask.utils import parse_bytes
from dask.sizeof import sizeof

from .utils_test import scaled_array_shape, timeseries_of_size


def test_scaled_array_shape():
    assert scaled_array_shape(1024, (2, "x"), dtype=bool) == (2, 512)
    assert scaled_array_shape(1024, (2, "x"), dtype=float) == (2, 64)
    assert scaled_array_shape(1024, (2, "x"), dtype=np.float64) == (2, 64)
    assert scaled_array_shape(1024, (2, "x")) == (2, 64)

    assert scaled_array_shape(16, ("x", "x"), dtype=bool) == (4, 4)
    assert scaled_array_shape(256, ("4x", "x"), dtype=bool) == (32, 8)
    assert scaled_array_shape(64, ("x", "x", "x"), dtype=float) == (2, 2, 2)

    assert scaled_array_shape("10kb", ("x", "1kb"), dtype=bool) == (10, 1000)


def sizeof_df(df):
    # Measure the size of each partition separately (each one has overhead of being a separate DataFrame)
    # TODO more efficient method than `df.partitions`? Use `dask.get` directly?
    parts = dask.compute(
        [df.partitions[i] for i in range(df.npartitions)], scheduler="threads"
    )
    return sum(map(sizeof, parts))


def test_timeseries_of_size():
    small_parts = timeseries_of_size(
        "1mb", freq="1s", partition_freq="100s", dtypes={"x": float}
    )
    big_parts = timeseries_of_size(
        "1mb", freq="1s", partition_freq="100s", dtypes={i: float for i in range(10)}
    )
    assert sizeof_df(small_parts) == pytest.approx(parse_bytes("1mb"), rel=0.1)
    assert sizeof_df(big_parts) == pytest.approx(parse_bytes("1mb"), rel=0.1)
    assert big_parts.npartitions < small_parts.npartitions
