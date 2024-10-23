import os
import uuid

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


@pytest.fixture(scope="module")
def cluster_name(request, scale):
    module = os.path.basename(request.fspath).split(".")[0]
    module = module.replace("test_", "")
    return f"geospatial-{module}-{scale}-{uuid.uuid4().hex[:8]}"


@pytest.fixture()
def client_factory(cluster_name, github_cluster_tags, benchmark_all):
    import contextlib

    @contextlib.contextmanager
    def _(n_workers, env=None, **cluster_kwargs):
        with coiled.Cluster(
            name=cluster_name,
            tags=github_cluster_tags,
            n_workers=n_workers,
            **cluster_kwargs,
        ) as cluster:
            if env:
                cluster.send_private_envs(env=env)
            with cluster.get_client() as client:
                # FIXME https://github.com/coiled/platform/issues/103
                client.wait_for_workers(n_workers)
                with benchmark_all(client):
                    yield client

    return _
