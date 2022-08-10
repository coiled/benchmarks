import uuid

from coiled import Cluster


def test_default_cluster_spinup_time(request, auto_benchmark_time):

    with Cluster(name=f"{request.node.originalname}-{uuid.uuid4().hex[:8]}"):
        pass
