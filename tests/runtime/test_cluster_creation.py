import uuid

from coiled import Cluster


def test_default_cluster_spinup_time(request, auto_benchmark_time, test_run_benchmark):
    """Note: this test must be kept in a separate module from the tests that use the
    small_cluster fixture (which has the scope=module) or its child small_client.
    This prevents having the small_cluster sitting idle for 5+ minutes while this test
    is running.
    """
    with Cluster(
        name=f"{request.node.originalname}-{uuid.uuid4().hex[:8]}", package_sync=True
    ) as cluster:
        cluster_id = cluster.cluster_id
        cluster_acc = cluster.account
        details_url = (
            f"https://cloud.coiled.io/{cluster_acc}/clusters/{cluster_id}/details"
        )

        test_run_benchmark.cluster_name = cluster.name
        test_run_benchmark.cluster_id = cluster_id
        # Replace corresponding lines in next coiled release where
        # details_url will be available as client.cluster.details_url
        # test_run_benchmark.cluster_details_url = client.cluster.details_url
        test_run_benchmark.cluster_details_url = details_url

        pass
