import os
import uuid
from typing import Any, Literal

import coiled
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--scale",
        action="store",
        default="small",
        help="Scale to run. Either 'small' or 'large'",
    )


@pytest.fixture(scope="session")
def scale(request):
    return request.config.getoption("scale")


def get_cluster_spec(scale: Literal["small", "large"]) -> dict[str, Any]:
    everywhere = dict(
        workspace="dask-engineering-gcp",
        region="us-central1",
        wait_for_workers=True,
        spot_policy="on-demand",
    )

    if scale == "small":
        return {
            "n_workers": 10,
            **everywhere,
        }
    elif scale == "large":
        return {
            "n_workers": 100,
            **everywhere,
        }


@pytest.fixture(scope="module")
def cluster_name(request, scale):
    module = os.path.basename(request.fspath).split(".")[0]
    module = module.replace("test_", "")
    return f"geospatial-{module}-{scale}-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def cluster(
    cluster_name,
    scale,
    github_cluster_tags,
):
    kwargs = dict(
        name=cluster_name,
        tags=github_cluster_tags,
        **get_cluster_spec(scale),
    )
    with coiled.Cluster(**kwargs) as cluster:
        yield cluster


@pytest.fixture()
def client(cluster, benchmark_all):
    with cluster.get_client() as client:
        with benchmark_all(client):
            yield client
