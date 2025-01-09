import os

import pytest

from tests.tpch.utils import get_dataset_path

pytestmark = pytest.mark.tpch_dask

dd = pytest.importorskip("dask.dataframe")


from . import dask_queries  # noqa: E402


@pytest.fixture(scope="session")
def dataset_path(local, scale):
    if local:
        # FIXME: pyarrow local fs is a bit odd. dask.dataframe should deal with this
        return "file://" + os.path.abspath(get_dataset_path(local, scale)) + "/"
    else:
        return get_dataset_path(local, scale)


@pytest.mark.shuffle_p2p
def test_query_01(client, dataset_path, fs, scale):
    dask_queries.query_01(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_02(client, dataset_path, fs, scale):
    dask_queries.query_02(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_03(client, dataset_path, fs, scale):
    dask_queries.query_03(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_04(client, dataset_path, fs, scale):
    dask_queries.query_04(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_05(client, dataset_path, fs, scale):
    dask_queries.query_05(dataset_path, fs, scale).compute()


def test_query_06(client, dataset_path, fs, scale):
    dask_queries.query_06(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_07(client, dataset_path, fs, scale):
    dask_queries.query_07(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_08(client, dataset_path, fs, scale):
    dask_queries.query_08(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_09(client, dataset_path, fs, scale):
    dask_queries.query_09(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_10(client, dataset_path, fs, scale):
    dask_queries.query_10(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_11(client, dataset_path, fs, scale):
    dask_queries.query_11(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_12(client, dataset_path, fs, scale):
    dask_queries.query_12(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_13(client, dataset_path, fs, scale):
    dask_queries.query_13(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_14(client, dataset_path, fs, scale):
    dask_queries.query_14(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_15(client, dataset_path, fs, scale):
    dask_queries.query_15(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_16(client, dataset_path, fs, scale):
    dask_queries.query_16(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_17(client, dataset_path, fs, scale):
    dask_queries.query_17(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_18(client, dataset_path, fs, scale):
    dask_queries.query_18(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_19(client, dataset_path, fs, scale):
    dask_queries.query_19(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_20(client, dataset_path, fs, scale):
    dask_queries.query_20(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_21(client, dataset_path, fs, scale):
    dask_queries.query_21(dataset_path, fs, scale).compute()


@pytest.mark.shuffle_p2p
def test_query_22(client, dataset_path, fs, scale):
    dask_queries.query_22(dataset_path, fs, scale).compute()
