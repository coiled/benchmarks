import pathlib

import pandas as pd
import pytest
from distributed import LocalCluster

from .generate_data import generate as generate_data

VERIFICATION_SCALE = 1

pytestmark = pytest.mark.tpch_correctness


@pytest.fixture(scope="module")
def data_path(tmp_path_factory):
    path = tmp_path_factory.mktemp("answers")
    scale = VERIFICATION_SCALE
    generate_data(scale=scale, path=path, relaxed_schema=True)
    return path / f"scale-{scale}"


@pytest.fixture(scope="module")
def cluster():
    with LocalCluster() as cluster:
        yield cluster


@pytest.fixture(scope="module")
def client(cluster, restart):
    with cluster.get_client() as client:
        if restart:
            client.restart()
        client.run(lambda: None)

        yield client


def get_expected_answer(query: int, answer_dir: pathlib.Path):
    answer = pd.read_csv(
        answer_dir / f"q{query}.out",
        sep="|",
    )
    answer = answer.rename(columns=lambda x: x.strip())
    if "o_orderdate" in answer.columns:
        answer["o_orderdate"] = pd.to_datetime(answer["o_orderdate"])

    return answer


def verify_result(result: pd.DataFrame, query: int, answer_dir: pathlib.Path):
    expected = get_expected_answer(query, answer_dir)
    result = result.reset_index(drop=True)

    # The expected answers are provided as whitespace-padded pipe-separated data.
    # We must therefore strip both the expected as well as the actual answer.
    for column, dtype in expected.dtypes.items():
        if pd.api.types.is_object_dtype(dtype):
            expected[column] = expected[column].apply(lambda x: x.strip())
            result[column] = result[column].apply(lambda x: x.strip())
    pd.testing.assert_frame_equal(result, expected, check_dtype=False)


@pytest.mark.tpch_correctness
@pytest.mark.parametrize("query", range(1, 8))
def test_dask_results(query, client, data_path):
    from . import test_dask

    func = getattr(test_dask, f"test_query_{query}")
    result = func(client, str(data_path) + "/", None)
    verify_result(result, query, pathlib.Path("./tests/tpch/answers/scale-1"))
