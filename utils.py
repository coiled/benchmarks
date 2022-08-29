from __future__ import annotations

import contextlib
import uuid

from coiled import Cluster
from distributed import Client


@contextlib.contextmanager
def small_cluster(
    backend_options=None,
    module=None,
):

    with Cluster(
        name=f"{module}-{uuid.uuid4().hex[:8]}",
        n_workers=10,
        worker_vm_types=["t3.large"],  # 2CPU, 8GiB
        scheduler_vm_types=["t3.large"],
        backend_options=backend_options,
    ) as cluster:
        yield cluster


@contextlib.contextmanager
def small_client(
    cluster=None,
    backend_options=None,
    module=None,
):
    stack = contextlib.ExitStack()
    if not cluster:
        cluster = stack.enter_context(small_cluster(backend_options, module))

    client = stack.enter_context(Client(cluster))
    with stack:
        cluster.scale(10)
        client.wait_for_workers(10)
        client.restart()
        yield client
