import pytest

import dask
import dask.array as da
from distributed import wait


@pytest.mark.stability
def test_spilling():
    with dask.config.set({"distributed.scheduler.allowed-failures": 0}):
        with Cluster(
            name=f"test_spilling-{uuid.uuid4().hex}",
            n_workers=5,
            worker_disk_size=50,
            wait_for_workers=True,
        ) as cluster:
            with Client(cluster) as client:
                data = da.random.random((200, 2**27))  # 200 GiB
                wait(data.persist())
                data.sum().compute()
