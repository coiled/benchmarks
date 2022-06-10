import uuid

from coiled import Cluster


def test_default_cluster_spinup_time(test_name):

    with Cluster(name=f"{test_name}-{uuid.uuid4().hex[:8]}"):
        pass
