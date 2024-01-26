import pathlib

import pandas as pd
import pytest
from distributed import LocalCluster

from .generate_answers import generate as generate_answers
from .generate_data import generate as generate_data

VERIFICATION_SCALE = 1

pytestmark = pytest.mark.tpch_correctness


@pytest.fixture(scope="module")
def answers_path(tmp_path_factory):
    path = tmp_path_factory.mktemp("answers")
    return generate_answers(base_path=path)


@pytest.fixture(scope="module")
def data_path(tmp_path_factory):
    path = tmp_path_factory.mktemp("data")
    scale = VERIFICATION_SCALE
    return pathlib.Path(generate_data(scale=scale, path=str(path), relaxed_schema=True))


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
@pytest.mark.parametrize(
    "query",
    [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        pytest.param(8, marks=pytest.mark.skip(reason="Not implemented")),
        pytest.param(9, marks=pytest.mark.xfail(reason="Result is wrong")),
        pytest.param(10, marks=pytest.mark.xfail(reason="Result is wrong")),
        11,
        pytest.param(12, marks=pytest.mark.xfail(reason="Result is wrong")),
        pytest.param(13, marks=pytest.mark.xfail(reason="Result is wrong")),
        pytest.param(14, marks=pytest.mark.xfail(reason="Result is wrong")),
        pytest.param(15, marks=pytest.mark.skip(reason="Not implemented")),
        pytest.param(16, marks=pytest.mark.skip(reason="Not implemented")),
        pytest.param(17, marks=pytest.mark.skip(reason="Not implemented")),
        pytest.param(18, marks=pytest.mark.skip(reason="Not implemented")),
        pytest.param(19, marks=pytest.mark.skip(reason="Not implemented")),
        pytest.param(20, marks=pytest.mark.skip(reason="Not implemented")),
        pytest.param(21, marks=pytest.mark.skip(reason="Not implemented")),
        pytest.param(22, marks=pytest.mark.skip(reason="Not implemented")),
    ],
)
def test_dask_results(query, client, answers_path, data_path):
    from . import test_dask

    func = getattr(test_dask, f"test_query_{query}")
    result = func(client, str(data_path) + "/", None)
    verify_result(result, query, answers_path)
