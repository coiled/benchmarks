from coiled import Cluster


def test_cluster_reconnect(small_cluster, get_cluster_info, benchmark_time):
    """How quickly can we reconnect to an existing cluster?"""
    with get_cluster_info(small_cluster), benchmark_time:
        with Cluster(name=small_cluster.name, shutdown_on_close=False):
            pass
