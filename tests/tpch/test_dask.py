import pytest

from . import dask_queries

pytestmark = pytest.mark.tpch_dask

dd = pytest.importorskip("dask_expr")


def test_query_1(client, dataset_path, fs):
    dask_queries.query_1(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_2(client, dataset_path, fs):
    dask_queries.query_2(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_3(client, dataset_path, fs):
    dask_queries.query_3(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_4(client, dataset_path, fs):
    dask_queries.query_4(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_5(client, dataset_path, fs):
    dask_queries.query_5(dataset_path, fs).compute()


def test_query_6(client, dataset_path, fs):
    dask_queries.query_6(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_7(client, dataset_path, fs):
    dask_queries.query_7(dataset_path, fs).compute()


def test_query_8(client, dataset_path, fs):
    dask_queries.query_8(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_9(client, dataset_path, fs):
    dask_queries.query_9(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_10(client, dataset_path, fs):
    dask_queries.query_10(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_11(client, dataset_path, fs):
    dask_queries.query_11(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_12(client, dataset_path, fs):
    dask_queries.query_12(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_13(client, dataset_path, fs):
    dask_queries.query_13(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_14(client, dataset_path, fs):
    dask_queries.query_14(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_15(client, dataset_path, fs):
    dask_queries.query_15(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_16(client, dataset_path, fs):
    dask_queries.query_16(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_17(client, dataset_path, fs):
    dask_queries.query_17(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_18(client, dataset_path, fs):
    dask_queries.query_18(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_19(client, dataset_path, fs):
    dask_queries.query_19(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_20(client, dataset_path, fs):
    dask_queries.query_20(dataset_path, fs).compute()


@pytest.mark.shuffle_p2p
def test_query_21(client, dataset_path, fs):
    dask_queries.query_21(dataset_path, fs).compute()


def test_query_22(client, dataset_path, fs):
    dask_queries.query_22(dataset_path, fs).compute()
