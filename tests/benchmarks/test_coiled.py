import uuid

import pytest
from coiled import Cluster


def test_default_cluster_spinup_time(request):

    with Cluster(name=f"{request.node.originalname}-{uuid.uuid4().hex[:8]}"):
        pass


@pytest.mark.parametrize("offset", [1, 2, 3, 4, 5])
def test_stuff(offset, benchmark_time):
    import random
    import time

    time.sleep(offset + random.uniform(-0.2, 0.2))
