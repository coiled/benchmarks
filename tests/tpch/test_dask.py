import os

import pytest

from tests.tpch.utils import get_dataset_path

pytestmark = pytest.mark.tpch_dask

dd = pytest.importorskip("dask.dataframe")


from . import dask_queries  # noqa: E402


@pytest.fixture(scope="session")
def dataset_path(local, scale):
    if local:
        # FIXME: pyarrow local fs is a bit odd. dask-expr should deal with this
        return "file://" + os.path.abspath(get_dataset_path(local, scale)) + "/"
    else:
        return get_dataset_path(local, scale)


@pytest.mark.shuffle_p2p
def test_query_15(client, dataset_path, fs, scale):
    dask_queries.query_15(dataset_path, fs, scale).compute()
