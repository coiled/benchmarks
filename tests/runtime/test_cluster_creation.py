import uuid

from coiled import Cluster


def test_default_cluster_spinup_time(request, auto_benchmark_time):
    """Note: this test must be kept in a separate module from the tests that use the
    small_cluster fixture (which has the scope=module) or its child small_client.
    This prevents having the small_cluster sitting idle for 5+ minutes while this test
    is running.
    """
    with Cluster(
        name=f"funtime-{request.node.originalname}-{uuid.uuid4().hex[:8]}",
        package_sync=True,
    ):
        pass
