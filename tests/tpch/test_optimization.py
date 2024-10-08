import pytest

from . import dask_queries

pytestmark = pytest.mark.tpch_dask


@pytest.fixture(
    params=[
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21,
        22,
    ],
)
def query(request):
    return request.param


def test_optimization(query, dataset_path, fs, client, scale):
    func = getattr(dask_queries, f"query_{query:02d}")
    result = func(dataset_path, fs, scale)
    # We need to inject .repartition(npartitions=1) which .compute() does under the hood
    result.repartition(npartitions=1).optimize()


@pytest.mark.skip(
    reason="This test does not work. See FIXME and https://github.com/dask/distributed/issues/8833."
)
def test_delay_computation_start(query, dataset_path, fs, client, scale):
    func = getattr(dask_queries, f"query_{query:02d}")
    result = func(dataset_path, fs, scale).optimize()
    # FIXME: Client.compute unblocks only until the graph is serialized and put onto
    # the comm buffer. It should wait until update_graph finishes, i.e. graph is
    # submitted, parsed, and the tasks have been added onto the scheduler.
    client.compute(result)
