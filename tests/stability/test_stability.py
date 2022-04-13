import sys
import uuid
from contextlib import ExitStack

import coiled
import dask
from dask.distributed import Client

SOFTWARE = f"dask-engineering/coiled_dist-py{sys.version_info[0]}{sys.version_info[1]}"


def test_shuffle_stability():
    with ExitStack() as stack:
        cluster = stack.enter_context(
            coiled.Cluster(
                software=SOFTWARE,
                name="coiled-stability-tests_" + str(uuid.uuid4()),
                account="dask-engineering",
                n_workers=10,
                backend_options={"spot": False},
            )
        )

        client = stack.enter_context(Client(cluster))  # noqa F841
        df = dask.datasets.timeseries(
            start="2000-01-01", end="2000-12-31", freq="1s", partition_freq="1D"
        )

        sdf = df.shuffle(on="x")

        sdf_size = sdf.memory_usage(index=True).sum() / (1024.0**3)
        assert sdf_size.compute() > 1.0  # 1.0 GB
