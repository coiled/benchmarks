from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import dask
import dask.dataframe as dd
from dask import datasets
from dask.distributed import wait

if TYPE_CHECKING:
    from dask.distributed import Client

try:
    import fastparquet
except ImportError:
    fastparquet = False

try:
    import pyarrow
except ImportError:
    pyarrow = False

FASTPARQUET_MARK = pytest.mark.skipif(
    not fastparquet, reason="fastparquet is not installed"
)
PYARROW_MARK = pytest.mark.skipif(not pyarrow, reason="pyarrow is not installed")


@pytest.fixture(
    params=[
        pytest.param("fastparquet", marks=FASTPARQUET_MARK),
        pytest.param("pyarrow", marks=PYARROW_MARK),
    ]
)
def engine(request):
    return request.param


def read_write_engines():
    """Cartesian product of engines returned as a parameterized mark."""
    return pytest.mark.parametrize(
        ("read_engine", "write_engine"),
        [
            pytest.param(
                "pyarrow", "fastparquet", marks=(FASTPARQUET_MARK, PYARROW_MARK)
            ),
            pytest.param(
                "fastparquet", "pyarrow", marks=(FASTPARQUET_MARK, PYARROW_MARK)
            ),
            pytest.param("pyarrow", "pyarrow", marks=PYARROW_MARK),
            pytest.param("fastparquet", "fastparquet", marks=FASTPARQUET_MARK),
        ],
    )


@pytest.mark.super_slow
def test_read_parquet_split_row_groups(engine: str, small_client: Client) -> None:
    """
    This dataset is ~600 GiB on disk. It has no _metadata. Individual files range from
    a few hundred MiB to a few GiB on disk, with varying numbers of row groups per file.
    This means we need to use `split_row_groups=True` to avoid too-large partitions.
    """
    kwargs: dict[str, bool | None] = {"split_row_groups": True}
    dask_version = tuple(
        int(v) for v in dask.__version__.split("+")[0].split(".", maxsplit=2)
    )
    if dask_version >= (2022, 4, 2):
        kwargs["parquet_file_extension"] = None
    else:
        kwargs["require_extension"] = False
    df = dd.read_parquet(
        "s3://daylight-openstreetmap/parquet/osm_elements/release=v1.10/**",
        engine=engine,
        **kwargs,
    )
    result = wait(df.persist())
    assert not result.not_done
    assert len(result.done) == df.npartitions
    # Use a loop here to show a useful failure message. all will short-circuit
    # if there are any False values in case there are a lot of tasks.
    if not all(r.status == "finished" for r in result.done):
        for res in result.done:
            if res.status != "finished":
                pytest.fail(res.result(), False)


def test_read_parquet(engine: str, small_client: Client) -> None:
    """
    This dataset is ~5 GiB on disk, although it will grow as Anaconda add more data.
    Each file is a few hundred MiB to a few GiB on disk with one row group per file.
    """
    df = dd.read_parquet(
        "s3://anaconda-package-data/conda/hourly/",
        engine=engine,
    )
    result = wait(df.persist())
    assert not result.not_done
    assert len(result.done) == df.npartitions
    # Use a loop here to show a useful failure message. all will short-circuit
    # if there are any False values in case there are a lot of tasks.
    if not all(r.status == "finished" for r in result.done):
        for res in result.done:
            if res.status != "finished":
                pytest.fail(res.result(), False)


def test_read_parquet_google_cloud(engine: str, small_client: Client) -> None:
    """This dataset lives on Google Cloud."""
    df = dd.read_parquet(
        "gcs://catalyst.coop/intake/test/hourly_emissions_epacems.parquet",
        engine=engine,
        storage_options={"anon": True},
    )
    result = wait(df.persist())
    assert not result.not_done
    assert len(result.done) == df.npartitions
    # Use a loop here to show a useful failure message. all will short-circuit
    # if there are any False values in case there are a lot of tasks.
    if not all(r.status == "finished" for r in result.done):
        for res in result.done:
            if res.status != "finished":
                pytest.fail(res.result(), False)


@read_write_engines()
def test_write_parquet(
    read_engine: str, write_engine: str, small_client: Client, s3_url: str
) -> None:
    df = datasets.timeseries()
    df.to_parquet(s3_url, engine=write_engine, write_metadata_file=False)
    df2 = dd.read_parquet(s3_url, engine=read_engine)

    result = wait(df2.persist())
    assert not result.not_done
    assert len(result.done) == df.npartitions
    # Use a loop here to show a useful failure message. all will short-circuit
    # if there are any False values in case there are a lot of tasks.
    if not all(r.status == "finished" for r in result.done):
        for res in result.done:
            if res.status != "finished":
                pytest.fail(res.result(), False)
