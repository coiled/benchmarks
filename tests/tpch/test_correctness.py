import os
import pathlib

import coiled
import dask
import pandas as pd
import pytest
from distributed import LocalCluster, performance_report

from .generate_answers import generate as generate_answers
from .utils import get_dataset_path

VERIFICATION_SCALE = 1

pytestmark = pytest.mark.tpch_correctness


@pytest.fixture(scope="module")
def answers_path(tmp_path_factory):
    path = tmp_path_factory.mktemp("answers")
    return generate_answers(base_path=path)


@pytest.fixture(scope="module")
def cluster(
    local,
    module,
    dask_env_variables,
    github_cluster_tags,
    name,
):
    if local:
        with LocalCluster() as cluster:
            yield cluster
    else:
        with dask.config.set({"distributed.scheduler.worker-saturation": "inf"}):
            with coiled.Cluster(
                name=f"tpch-{module}-{VERIFICATION_SCALE}-{name}",
                environ=dask_env_variables,
                tags=github_cluster_tags,
                region="us-east-2",
                worker_vm_types=["m6i.large"],  # 2 CPUs, 8GiB RAM
                n_workers=2,
                wait_for_workers=True,
                scheduler_vm_types=["m6i.large"],
                spot_policy="spot_with_fallback",
            ) as cluster:
                yield cluster


@pytest.fixture
def client(
    request,
    cluster,
    testrun_uid,
    cluster_kwargs,
    benchmark_time,
    restart,
    local,
    query,
):
    with cluster.get_client() as client:
        if restart:
            client.restart()
        client.run(lambda: None)

        local = "local" if local else "cloud"
        if request.config.getoption("--performance-report"):
            if not os.path.exists("performance-reports"):
                os.mkdir("performance-reports")
            with performance_report(
                filename=os.path.join(
                    "performance-reports", f"{local}-{VERIFICATION_SCALE}-{query}.html"
                )
            ):
                with benchmark_time:
                    yield client
        else:
            with benchmark_time:
                yield client


def get_expected_answer(query: int, answer_dir: pathlib.Path):
    answer = pd.read_csv(
        answer_dir / f"q{query}.out",
        sep="|",
    )
    answer = answer.rename(columns=lambda x: x.strip())
    if "o_orderdate" in answer.columns:
        answer["o_orderdate"] = pd.to_datetime(answer["o_orderdate"])
    if "cntrycode" in answer.columns:
        answer["cntrycode"] = answer["cntrycode"].astype(str)

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
    pd.testing.assert_frame_equal(result, expected, check_dtype=False, atol=1e-3)


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
def test_dask_results(query, local, answers_path, client):
    from . import dask_queries

    func = getattr(dask_queries, f"query_{query}")
    result = func(
        get_dataset_path(local, VERIFICATION_SCALE), None, VERIFICATION_SCALE
    ).compute()
    verify_result(result, query, answers_path)
