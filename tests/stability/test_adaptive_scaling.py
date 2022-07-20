import pytest
import time
import uuid

import dask.array as da
from coiled.v2 import Cluster
from dask.distributed import Client, Event, Semaphore

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
            adapt = cluster.adapt(minimum=minimum, maximum=10)
            time.sleep(adapt.interval * 2.1) # Ensure enough time for system to adapt 
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


@pytest.mark.stability
@pytest.mark.parametrize("minimum", (0, 1))
def test_adapt_to_changing_workload(minimum: int):
    maximum = 20
    fan_out_size = 100
    with Cluster(
        name=f"test_adaptive_scaling-{uuid.uuid4().hex}",
        n_workers=10,
        worker_vm_types=["t3.medium"],
    ) as cluster:
        with Client(cluster) as client:
            adapt = cluster.adapt(minimum=minimum, maximum=maximum)
            assert len(adapt.log) == 0

            def clog(x: int, ev: Event, sem: Semaphore, **kwargs) -> int:
                # Ensure that no recomputation happens by decrementing a countdown on a semaphore
                acquired = sem.acquire(timeout=0.1)
                assert acquired is True
                ev.wait()
                return x

            sem_fan_out = Semaphore(name="fan-out", max_leases=fan_out_size)
            ev_fan_out = Event(name="fan-out", client=client)

            fan_out = client.map(clog, range(fan_out_size), ev=ev_fan_out, sem=sem_fan_out)

            reduction = client.submit(sum, fan_out)
            sem_barrier = Semaphore(name="barrier", max_leases=1)
            ev_barrier = Event(name="barrier", client=client)
            barrier = client.submit(clog, reduction, ev=ev_barrier, sem=sem_barrier)
            
            sem_final_fan_out = Semaphore(name="final-fan-out", max_leases=fan_out_size)
            ev_final_fan_out = Event(name="final-fan-out", client=client)
            final_fan_out = client.map(clog, range(fan_out_size), ev=ev_final_fan_out, sem=sem_final_fan_out, barrier=barrier)

            # Scale up to maximum
            client.wait_for_workers(n_workers=maximum, timeout=300)
            assert len(cluster.observed) == maximum
            assert adapt.log[-1][1]["status"] == "up"

            ev_fan_out.set()
            # Scale down to a single worker
            start = time.monotonic()
            while len(cluster.observed) > 1:
                time.sleep(0.1)
            end = time.monotonic()
            assert len(cluster.observed) == 1
            assert adapt.log[-1][1]["status"] == "down"
            # Do not take longer than 5 minutes to scale down
            assert end - start < 300
            
            ev_barrier.set()
            # Scale up to maximum again
            client.wait_for_workers(n_workers=maximum, timeout=300)
            while len(cluster.observed) < maximum:
                time.sleep(0.1)
            assert len(cluster.observed) == 20
            assert adapt.log[-1][1]["status"] == "up"

            ev_final_fan_out.set()
            client.gather(final_fan_out)

            # Scale down to minimum
            start = time.monotonic()
            while len(cluster.observed) > minimum:
                time.sleep(0.1)
            end = time.monotonic()
            assert len(cluster.observed) == minimum
            assert adapt.log[-1][1]["status"] == "down"
            # Do not take longer than 5 minutes to scale down
            assert end - start < 300