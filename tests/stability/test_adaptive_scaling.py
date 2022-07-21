import time
import uuid

import dask.array as da
import pytest
from coiled.v2 import Cluster
from dask import delayed
from dask.distributed import Client, Event, Semaphore, wait


@pytest.mark.stability
@pytest.mark.parametrize("minimum", (0, 1))
@pytest.mark.parametrize(
    "scatter",
    (
        False,
        pytest.param(True, marks=[pytest.mark.xfail(reason="dask/distributed#6686")]),
    ),
)
def test_scale_up_on_task_load(minimum, scatter):
    maximum = 10
    with Cluster(
        name=f"test_adaptive_scaling-{uuid.uuid4().hex}",
        n_workers=minimum,
        worker_vm_types=["t3.medium"],
    ) as cluster:
        with Client(cluster) as client:
            assert len(cluster.observed) == minimum
            adapt = cluster.adapt(minimum=minimum, maximum=maximum)
            time.sleep(adapt.interval * 2.1)  # Ensure enough time for system to adapt
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
            client.wait_for_workers(n_workers=maximum, timeout=360)
            assert len(adapt.log) <= 2
            assert adapt.log[-1][1] == {"status": "up", "n": maximum}
            ev_fan_out.set()
            client.gather(futures)


@pytest.mark.stability
@pytest.mark.parametrize("minimum", (0, 1))
def test_adapt_to_changing_workload(minimum: int):
    maximum = 10
    fan_out_size = 100
    with Cluster(
        name=f"test_adaptive_scaling-{uuid.uuid4().hex}",
        n_workers=5,
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

            fan_out = client.map(
                clog, range(fan_out_size), ev=ev_fan_out, sem=sem_fan_out
            )

            reduction = client.submit(sum, fan_out)
            sem_barrier = Semaphore(name="barrier", max_leases=1)
            ev_barrier = Event(name="barrier", client=client)
            barrier = client.submit(clog, reduction, ev=ev_barrier, sem=sem_barrier)

            sem_final_fan_out = Semaphore(name="final-fan-out", max_leases=fan_out_size)
            ev_final_fan_out = Event(name="final-fan-out", client=client)
            final_fan_out = client.map(
                clog,
                range(fan_out_size),
                ev=ev_final_fan_out,
                sem=sem_final_fan_out,
                barrier=barrier,
            )

            # Scale up to maximum
            client.wait_for_workers(n_workers=maximum, timeout=420)
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
            assert end - start < 420

            ev_barrier.set()
            # Scale up to maximum again
            client.wait_for_workers(n_workers=maximum, timeout=420)
            while len(cluster.observed) < maximum:
                time.sleep(0.1)
            assert len(cluster.observed) == maximum
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
            assert end - start < 420


@pytest.mark.stability
@pytest.mark.parametrize("minimum", (0, 1))
def test_adapt_to_memory_intensive_workload(minimum):
    maximum = 10
    with Cluster(
        name=f"test_adaptive_scaling-{uuid.uuid4().hex}",
        n_workers=minimum,
        worker_vm_types=["t3.medium"],
    ) as cluster:
        with Client(cluster) as client:
            assert len(cluster.observed) == minimum
            adapt = cluster.adapt(minimum=minimum, maximum=maximum)
            assert len(adapt.log) == 0

            def memory_intensive_preprocessing():
                matrix = da.random.random((48000, 48000))
                rechunked = matrix.rechunk((matrix.shape[0], 200)).rechunk(
                    (200, matrix.shape[1])
                )
                reduction = rechunked.sum()
                return reduction

            @delayed
            def clog(x, ev: Event):
                ev.wait()
                return x

            def compute_intensive_barrier_task(data, ev: Event):
                barrier = clog(data, ev)
                return barrier

            def memory_intensive_postprocessing(data):
                matrix = da.random.random((48000, 48000))
                matrix = matrix + da.from_delayed(data, shape=(1,), dtype="float")
                rechunked = matrix.rechunk((matrix.shape[0], 200)).rechunk(
                    (200, matrix.shape[1])
                )
                reduction = rechunked.sum()
                return reduction

            ev_barrier = Event(name="barrier", client=client)

            fut = client.compute(
                memory_intensive_postprocessing(
                    compute_intensive_barrier_task(
                        memory_intensive_preprocessing(), ev_barrier
                    )
                )
            )

            # Scale up to maximum on preprocessing
            client.wait_for_workers(n_workers=maximum, timeout=360)
            assert len(cluster.observed) == maximum
            assert adapt.log[-1][1]["status"] == "up"

            # Scale down to a single worker on barrier task
            start = time.monotonic()
            while len(cluster.observed) > 1:
                time.sleep(0.1)
            end = time.monotonic()
            assert len(cluster.observed) == 1
            assert adapt.log[-1][1]["status"] == "down"
            assert end - start < 420

            ev_barrier.set()

            # Scale up to maximum on postprocessing
            client.wait_for_workers(n_workers=maximum, timeout=360)
            assert len(cluster.observed) == maximum
            assert adapt.log[-1][1]["status"] == "up"

            wait(fut)
            del fut

            # Scale down to minimum
            start = time.monotonic()
            while len(cluster.observed) > minimum:
                time.sleep(0.1)
            end = time.monotonic()
            assert len(cluster.observed) == minimum
            assert adapt.log[-1][1]["status"] == "down"
            assert end - start < 420
