import uuid

from coiled import Cluster


def test_default_cluster_spinup_time(
    auto_benchmark_time, test_run_benchmark, gitlab_cluster_tags
):
    """Note: this test must be kept in a separate module from the tests that use the
    small_cluster fixture (which has the scope=module) or its child small_client.
    This prevents having the small_cluster sitting idle for 5+ minutes while this test
    is running.
    """
    with Cluster(
        name=f"test_default_cluster_spinup_time-{uuid.uuid4().hex[:8]}",
        n_workers=1,
        package_sync=True,
        tags=gitlab_cluster_tags,
    ) as cluster:
        test_run_benchmark.cluster_name = cluster.name
        test_run_benchmark.cluster_id = cluster.cluster_id
        test_run_benchmark.cluster_details_url = cluster.details_url

        pass
