import time
import uuid

from coiled import Cluster


def test_default_cluster_spinup_time(request):
    time.sleep(10)
    with Cluster(name=f"{request.node.originalname}-{uuid.uuid4().hex[:8]}"):
        pass
