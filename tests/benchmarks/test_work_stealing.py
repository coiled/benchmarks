import dask.array as da
import numpy as np
from dask import delayed, utils
from distributed import wait, Client
from coiled.v2 import Cluster
import time

@pytest.mark.xfail()
def test_trivial_workload_should_not_cause_work_stealing(small_client):
    root = delayed(lambda n: "x" * n)(utils.parse_bytes("1MiB"), dask_key_name="root")
    results = [delayed(lambda *args: None)(root, i) for i in range(10000)]
    fut = small_client.compute(results)
    wait(fut)
    def count_work_stealing_events(dask_scheduler):
        return len(dask_scheduler.events["stealing"])

    count = small_client.run_on_scheduler(count_work_stealing_events)
    assert count == 0


def test_work_stealing_on_scaling_up(test_name_uuid, benchmark_task_durations, benchmark_memory, benchmark_time):
    with Cluster(name=test_name_uuid, n_workers=1, wait_for_workers=True, worker_vm_types=["t3.medium"]) as cluster:
        with Client(cluster) as client:
            # Slow task.
            def func1(chunk):
                if sum(chunk.shape) != 0: # Make initialization fast
                    time.sleep(5)
                return chunk

            def func2(chunk):
                return chunk

            data = da.zeros((30, 30, 30), chunks=5)
            result = data.map_overlap(func1, depth=1, dtype=data.dtype)
            result = result.map_overlap(func2, depth=1, dtype=data.dtype)
            future = client.compute(result)

            print('started computation')

            time.sleep(11)
            # print('scaling to 4 workers')
            # client.cluster.scale(4)

            time.sleep(5)
            print('scaling to 20 workers')
            cluster.scale(20)

            _ = future.result()


def test_work_stealing_on_inhomogeneous_workload(small_client):
    np.random.seed(42)
    delays = np.random.lognormal(1, 1.3, 500)

    @delayed
    def clog(n):
        time.sleep(min(n, 60))
        return n
    
    results = [clog(i) for i in delays]
    fut = small_client.compute(results)
    wait(fut)
