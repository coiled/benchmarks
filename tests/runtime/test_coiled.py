import pytest
from coiled import Cluster


@pytest.mark.cluster("small")
def test_cluster_reconnect(cluster, get_cluster_info, benchmark_time):
    """How quickly can we reconnect to an existing cluster?"""
    with get_cluster_info(cluster), benchmark_time:
        with Cluster(name=cluster.name, shutdown_on_close=False):
            pass
