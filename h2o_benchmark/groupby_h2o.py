"""
h2o-ai benchmark groupby part running on coiled.
"""
import os
import timeit

import coiled
import dask.dataframe as dd
from dask.distributed import Client

SOFTWARE = os.environ["SOFTWARE_ENV"]

tic_cluster = timeit.default_timer()
cluster = coiled.Cluster(
    software=SOFTWARE,
    account="dask-engineering",
    n_workers=4,
    backend_options={"spot": False},
)
toc_cluster = timeit.default_timer()

time_cluster = toc_cluster - tic_cluster

client = Client(cluster)

tic_read_gby = timeit.default_timer()

ddf = dd.read_csv(
    "s3://coiled-datasets/h2o-benchmark/N_1e7_K_1e2_single.csv",
    dtype={
        "id1": "category",
        "id2": "category",
        "id3": "category",
        "id4": "Int32",
        "id5": "Int32",
        "id6": "Int32",
        "v1": "Int32",
        "v2": "Int32",
        "v3": "float64",
    },
    storage_options={"anon": True},
)

# q1: sum v1 by id1
ddf.groupby("id1", dropna=False, observed=True).agg({"v1": "sum"}).compute()

# q2: sum v1 by id1:id2
ddf.groupby(["id1", "id2"], dropna=False, observed=True).agg({"v1": "sum"}).compute()

# q3: sum v1 mean v3 by id3
ddf.groupby("id3", dropna=False, observed=True).agg(
    {"v1": "sum", "v3": "mean"}
).compute()


toc_read_gby = timeit.default_timer()

# Note that we are reading bunch of times here. Not sure this is what we want.
# we could do something like dask.compute(gby_1, gby2, ...)
time_read_gby = toc_read_gby - tic_read_gby

# save this times somewhere, maybe for now print them.
print(f"{time_cluster= }")
print(f"{time_read_gby= }")

client.close()
cluster.close()
