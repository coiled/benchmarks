import uuid

from coiled import Cluster


def test_default_cluster_spinup_time(
    benchmark_time, github_cluster_tags, get_cluster_info
):
    """Note: this test must be kept in a separate module from the tests that use the
    small_cluster fixture (which has the scope=module) or its child small_client.
    This prevents having the small_cluster sitting idle for 5+ minutes while this test
    is running.
    """
    with benchmark_time:
        with Cluster(
            name=f"test_default_cluster_spinup_time-{uuid.uuid4().hex[:8]}",
            n_workers=1,
            tags=github_cluster_tags,
        ) as cluster:
            with get_cluster_info(cluster):
                pass
