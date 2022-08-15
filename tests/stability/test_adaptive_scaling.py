import time
import uuid

import dask
import dask.array as da
import pytest
from coiled.v2 import Cluster
from dask import delayed
from dask.distributed import Client, Event, wait

TIMEOUT_THRESHOLD = 1800  # 10 minutes


@pytest.mark.stability
@pytest.mark.parametrize("minimum,threshold", [(0, 300), (1, 150)])
@pytest.mark.parametrize(
    "scatter",
    (
        False,
        pytest.param(True, marks=[pytest.mark.xfail(reason="dask/distributed#6686")]),
    ),
)
def test_scale_up_on_task_load(minimum, threshold, scatter):
    """Tests that adaptive scaling reacts in a reasonable amount of time to
    an increased task load and scales up.
    """
    maximum = 10
    with Cluster(
        name=f"test_adaptive_scaling-{uuid.uuid4().hex}",
        n_workers=minimum,
        worker_vm_types=["t3.medium"],
        wait_for_workers=True,
        # Note: We set allowed-failures to ensure that no tasks are not retried upon ungraceful shutdown behavior
        # during adaptive scaling but we receive a KilledWorker() instead.
        environ={"DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES": 0},
    ) as cluster:
        with Client(cluster) as client:
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

            end = time.monotonic()
            client.wait_for_workers(n_workers=maximum, timeout=TIMEOUT_THRESHOLD)
            start = time.monotonic()
            duration = end - start
            assert duration < threshold, duration
            assert len(adapt.log) <= 2
            assert adapt.log[-1][1] == {"status": "up", "n": maximum}
            ev_fan_out.set()
            client.gather(futures)
            return duration


@pytest.mark.stability
@pytest.mark.parametrize("minimum", (0, 1))
def test_adapt_to_changing_workload(minimum: int):
    """Tests that adaptive scaling reacts within a reasonable amount of time to
    a varying task load and scales up or down. This also asserts that no recomputation
    is caused by the scaling.
    """
    maximum = 10
    fan_out_size = 100
    with Cluster(
        name=f"test_adaptive_scaling-{uuid.uuid4().hex}",
        n_workers=5,
        worker_vm_types=["t3.medium"],
        wait_for_workers=True,
        # Note: We set allowed-failures to ensure that no tasks are not retried upon ungraceful shutdown behavior
        # during adaptive scaling but we receive a KilledWorker() instead.
        environ={"DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES": 0},
    ) as cluster:
        with Client(cluster) as client:
            adapt = cluster.adapt(minimum=minimum, maximum=maximum)
            assert len(adapt.log) == 0

            @delayed
            def clog(x: int, ev: Event, **kwargs) -> int:
                ev.wait()
                return x

            def workload(
                fan_out_size,
                ev_fan_out,
                ev_barrier,
                ev_final_fan_out,
            ):
                fan_out = [clog(i, ev=ev_fan_out) for i in range(fan_out_size)]
                barrier = clog(delayed(sum)(fan_out), ev=ev_barrier)
                final_fan_out = [
                    clog(i, ev=ev_final_fan_out, barrier=barrier)
                    for i in range(fan_out_size)
                ]
                return final_fan_out

            ev_fan_out = Event(name="fan-out", client=client)
            ev_barrier = Event(name="barrier", client=client)
            ev_final_fan_out = Event(name="final-fan-out", client=client)

            fut = client.compute(
                workload(
                    fan_out_size=fan_out_size,
                    ev_fan_out=ev_fan_out,
                    ev_barrier=ev_barrier,
                    ev_final_fan_out=ev_final_fan_out,
                )
            )

            # Scale up to maximum
            start = time.monotonic()
            client.wait_for_workers(n_workers=maximum, timeout=TIMEOUT_THRESHOLD)
            end = time.monotonic()
            duration_first_scale_up = end - start
            assert duration_first_scale_up < 150
            assert len(cluster.observed) == maximum
            assert adapt.log[-1][1]["status"] == "up"

            ev_fan_out.set()
            # Scale down to a single worker
            start = time.monotonic()
            while len(cluster.observed) > 1:
                if time.monotonic() - start >= TIMEOUT_THRESHOLD:
                    raise TimeoutError()
                time.sleep(0.1)
            end = time.monotonic()
            duration_first_scale_down = end - start
            assert duration_first_scale_down < 330
            assert len(cluster.observed) == 1
            assert adapt.log[-1][1]["status"] == "down"

            ev_barrier.set()
            # Scale up to maximum again
            start = time.monotonic()
            client.wait_for_workers(n_workers=maximum, timeout=TIMEOUT_THRESHOLD)
            end = time.monotonic()
            duration_second_scale_up = end - start
            assert duration_second_scale_up < 150
            assert len(cluster.observed) == maximum
            assert adapt.log[-1][1]["status"] == "up"

            ev_final_fan_out.set()
            client.gather(fut)
            del fut

            # Scale down to minimum
            start = time.monotonic()
            while len(cluster.observed) > minimum:
                if time.monotonic() - start >= TIMEOUT_THRESHOLD:
                    raise TimeoutError()
                time.sleep(0.1)
            end = time.monotonic()
            duration_second_scale_down = end - start
            assert duration_second_scale_down < 330
            assert len(cluster.observed) == minimum
            assert adapt.log[-1][1]["status"] == "down"
            return (
                duration_first_scale_up,
                duration_first_scale_down,
                duration_second_scale_up,
                duration_second_scale_down,
            )


@pytest.mark.skip(
    reason="The test behavior is unreliable and may lead to very long runtime (see: coiled-runtime#211)"
)
@pytest.mark.stability
@pytest.mark.parametrize("minimum", (0, 1))
def test_adapt_to_memory_intensive_workload(minimum):
    """Tests that adaptive scaling reacts within a reasonable amount of time to a varying task and memory load.

    Note: This tests currently results in spilling and very long runtimes.
    """
    maximum = 10
    with Cluster(
        name=f"test_adaptive_scaling-{uuid.uuid4().hex}",
        n_workers=minimum,
        worker_vm_types=["t3.medium"],
        wait_for_workers=True,
        # Note: We set allowed-failures to ensure that no tasks are not retried upon ungraceful shutdown behavior
        # during adaptive scaling but we receive a KilledWorker() instead.
        environ={"DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES": 0},
    ) as cluster:
        with Client(cluster) as client:
            adapt = cluster.adapt(minimum=minimum, maximum=maximum)
            assert len(adapt.log) == 0

            def memory_intensive_processing():
                matrix = da.random.random((40000, 40000), chunks=(40000, 500))
                rechunked = matrix.rechunk((500, 40000))
                reduction = rechunked.sum()
                return reduction

            @delayed
            def clog(x, ev_start: Event, ev_barrier: Event):
                ev_start.set()
                ev_barrier.wait()
                return x

            def compute_intensive_barrier_task(
                data, ev_start: Event, ev_barrier: Event
            ):
                barrier = clog(data, ev_start, ev_barrier)
                return barrier

            ev_scale_down = Event(name="scale_down", client=client)
            ev_barrier = Event(name="barrier", client=client)

            fut = client.compute(
                compute_intensive_barrier_task(
                    memory_intensive_processing(), ev_scale_down, ev_barrier
                )
            )

            # Scale up to maximum on preprocessing
            start = time.monotonic()
            client.wait_for_workers(n_workers=maximum, timeout=TIMEOUT_THRESHOLD)
            end = time.monotonic()
            duration_first_scale_up = end - start
            assert duration_first_scale_up < 420, duration_first_scale_up
            assert len(cluster.observed) == maximum
            assert adapt.log[-1][1]["status"] == "up"

            ev_scale_down.wait()
            # Scale down to a single worker on barrier task
            start = time.monotonic()
            while len(cluster.observed) > 1:
                if time.monotonic() - start >= TIMEOUT_THRESHOLD:
                    raise TimeoutError()
                time.sleep(0.1)
            end = time.monotonic()
            duration_first_scale_down = end - start
            assert duration_first_scale_down < 420, duration_first_scale_down
            assert len(cluster.observed) == 1
            assert adapt.log[-1][1]["status"] == "down"

            ev_barrier.set()
            wait(fut)
            fut = client.compute(memory_intensive_processing())

            # Scale up to maximum on postprocessing
            start = time.monotonic()
            client.wait_for_workers(n_workers=maximum, timeout=TIMEOUT_THRESHOLD)
            end = time.monotonic()
            duration_second_scale_up = end - start
            assert duration_second_scale_up < 420, duration_second_scale_up
            assert len(cluster.observed) == maximum
            assert adapt.log[-1][1]["status"] == "up"

            wait(fut)
            del fut

            # Scale down to minimum
            start = time.monotonic()
            while len(cluster.observed) > minimum:
                if time.monotonic() - start >= TIMEOUT_THRESHOLD:
                    raise TimeoutError()
                time.sleep(0.1)
            end = time.monotonic()
            duration_second_scale_down = end - start
            assert duration_second_scale_down < 420, duration_second_scale_down
            assert len(cluster.observed) == minimum
            assert adapt.log[-1][1]["status"] == "down"
            return (
                duration_first_scale_up,
                duration_first_scale_down,
                duration_second_scale_up,
                duration_second_scale_down,
            )
