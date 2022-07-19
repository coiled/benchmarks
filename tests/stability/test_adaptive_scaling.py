import uuid
import pytest
from coiled.v2 import Cluster
from dask.distributed import Client
from distributed import Event
import time


@pytest.mark.stability
@pytest.mark.parametrize("minimum", (0, 1))
@pytest.mark.parametrize("scatter", (False, pytest.param(True, marks=[pytest.mark.xfail("dask/distributed#6686")])))
def test_scale_up_on_task_load(minimum, scatter):
     with Cluster(
        name=f"test_adaptive_scaling-{uuid.uuid4().hex}",
        n_workers=minimum,
        worker_vm_types=["t3.medium"],
    ) as cluster:
        with Client(cluster) as client:
            assert len(cluster.observed) == minimum
            adapt = cluster.adapt(minimum=minimum, maximum=10, interval="5s", wait_count=3)
            time.sleep(10)
            assert len(adapt.log) == 0
            ev_fan_out = Event(name="fan-out", client=client)

            def clog(x: int, ev: Event) -> int:
                ev.wait()
                return x

            numbers = range(100)
            if scatter is True:
                numbers = client.scatter(list(numbers))

            futures = client.map(clog, numbers, ev=ev_fan_out)

            # Scale up within 5 minutes
            client.wait_for_workers(n_workers=10, timeout=300)
            assert len(adapt.log) <= 2
            assert adapt.log[-1][1] == {"status": "up", "n": 10}
            ev_fan_out.set()
            client.gather(futures)

