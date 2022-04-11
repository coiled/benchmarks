"""
h2o-ai benchmark groupby part running on coiled.
"""
import datetime
import os
import timeit
import uuid

import coiled
import dask.dataframe as dd
import pandas
import pandas as pd
from dask.distributed import Client
from distributed.diagnostics import SchedulerPlugin
from distributed.utils import key_split, key_split_group


class TaskGroupStatistics(SchedulerPlugin):
    def __init__(self):
        """Initialize the plugin"""
        self.groups = {}
        self.scheduler = None

    def start(self, scheduler):
        """Called on scheduler start as well as on registration time"""
        self.scheduler = scheduler
        scheduler.handlers["get_task_groups"] = self.get_task_groups

    def transition(self, key, start, finish, *args, **kwargs):
        """On key transition to memory, update the task group data"""
        if self.scheduler is None:
            # Should not get here if initialization has happened correctly
            return

        if start == "processing" and finish == "memory":
            prefix_name = key_split(key)
            group_name = key_split_group(key)

            if group_name not in self.groups:
                self.groups[group_name] = {}

            group = self.scheduler.task_groups[group_name]
            self.groups[group_name]["prefix"] = prefix_name
            self.groups[group_name]["duration"] = group.duration
            self.groups[group_name]["start"] = str(
                datetime.datetime.fromtimestamp(group.start)
            )
            self.groups[group_name]["stop"] = str(
                datetime.datetime.fromtimestamp(group.stop)
            )
            self.groups[group_name]["nbytes"] = group.nbytes_total

    async def get_task_groups(self, comm):
        return self.groups

    def restart(self, scheduler):
        self.groups = {}


SOFTWARE = os.environ["SOFTWARE_ENV"]

tic_cluster = timeit.default_timer()
cluster = coiled.Cluster(
    software=SOFTWARE,
    name="h2o-groupby-benchmark_" + str(uuid.uuid4()),
    account="dask-engineering",
    n_workers=4,
    backend_options={"spot": False},
)
toc_cluster = timeit.default_timer()

time_cluster = toc_cluster - tic_cluster

client = Client(cluster)

client.register_scheduler_plugin(TaskGroupStatistics())


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
def q1(ddf):
    return ddf.groupby("id1", dropna=False, observed=True).agg({"v1": "sum"}).compute()


def q2(ddf):
    return (
        ddf.groupby(["id1", "id2"], dropna=False, observed=True)
        .agg({"v1": "sum"})
        .compute()
    )


def q3(ddf):
    return (
        ddf.groupby("id3", dropna=False, observed=True)
        .agg({"v1": "sum", "v3": "mean"})
        .compute()
    )


def q4(ddf):
    return (
        ddf.groupby("id4", dropna=False, observed=True)
        .agg({"v1": "mean", "v2": "mean", "v3": "mean"})
        .compute()
    )


def q5(ddf):
    return (
        ddf.groupby("id6", dropna=False, observed=True)
        .agg({"v1": "sum", "v2": "sum", "v3": "sum"})
        .compute()
    )


def q7(ddf):
    return (
        ddf.groupby("id3", dropna=False, observed=True)
        .agg({"v1": "max", "v2": "min"})
        .assign(range_v1_v2=lambda x: x["v1"] - x["v2"])[["range_v1_v2"]]
        .compute()
    )


def q8(ddf):
    return (
        ddf[~ddf["v3"].isna()][["id6", "v3"]]
        .groupby("id6", dropna=False, observed=True)
        .apply(
            lambda x: x.nlargest(2, columns="v3"),
            meta={"id6": "Int64", "v3": "float64"},
        )[["v3"]]
        .compute()
    )


def q9(ddf):
    return (
        ddf[["id2", "id4", "v1", "v2"]]
        .groupby(["id2", "id4"], dropna=False, observed=True)
        .apply(
            lambda x: pd.Series({"r2": x.corr()["v1"]["v2"] ** 2}),
            meta={"r2": "float64"},
        )
        .compute()
    )


# q1
tic_q1 = timeit.default_timer()
q1(ddf)
toc_q1 = timeit.default_timer()

q1_stats = client.sync(client.scheduler.get_task_groups)

df_q1 = pandas.DataFrame.from_dict(q1_stats, orient="index")
df_q1.index.name = "group"

print("q1 STATS")
print(df_q1.to_string(index=False))

client.restart()

# q2
tic_q2 = timeit.default_timer()
q2(ddf)
toc_q2 = timeit.default_timer()

q2_stats = client.sync(client.scheduler.get_task_groups)

df_q2 = pandas.DataFrame.from_dict(q2_stats, orient="index")
df_q2.index.name = "group"

print("q2 STATS")
print(df_q2.to_string(index=False))

# q3
tic_q3 = timeit.default_timer()
q3(ddf)
toc_q3 = timeit.default_timer()

q3_stats = client.sync(client.scheduler.get_task_groups)

df_q3 = pandas.DataFrame.from_dict(q3_stats, orient="index")
df_q3.index.name = "group"

print("q3 STATS")
print(df_q3.to_string(index=False))

# q4
tic_q4 = timeit.default_timer()
q4(ddf)
toc_q4 = timeit.default_timer()

q4_stats = client.sync(client.scheduler.get_task_groups)

df_q4 = pandas.DataFrame.from_dict(q4_stats, orient="index")
df_q4.index.name = "group"

print("q4 STATS")
print(df_q4.to_string(index=False))

# q5
tic_q5 = timeit.default_timer()
q5(ddf)
toc_q5 = timeit.default_timer()

q5_stats = client.sync(client.scheduler.get_task_groups)

df_q5 = pandas.DataFrame.from_dict(q5_stats, orient="index")
df_q5.index.name = "group"

print("q5 STATS")
print(df_q5.to_string(index=False))

# q7 q6 is skipped in original benchmark
tic_q7 = timeit.default_timer()
q7(ddf)
toc_q7 = timeit.default_timer()

q7_stats = client.sync(client.scheduler.get_task_groups)

df_q7 = pandas.DataFrame.from_dict(q7_stats, orient="index")
df_q7.index.name = "group"

print("q7 STATS")
print(df_q7.to_string(index=False))

# q8
tic_q8 = timeit.default_timer()
q8(ddf)
toc_q8 = timeit.default_timer()

q8_stats = client.sync(client.scheduler.get_task_groups)

df_q8 = pandas.DataFrame.from_dict(q8_stats, orient="index")
df_q8.index.name = "group"

print("q8 STATS")
print(df_q8.to_string(index=False))

# q9
tic_q9 = timeit.default_timer()
q9(ddf)
toc_q9 = timeit.default_timer()

q9_stats = client.sync(client.scheduler.get_task_groups)

df_q9 = pandas.DataFrame.from_dict(q9_stats, orient="index")
df_q9.index.name = "group"

print("q9 STATS")
print(df_q9.to_string(index=False))

# save this times somewhere, maybe for now print them.
print("\n")
print(f"{time_cluster= }")
print(f"time q1 = {toc_q1-tic_q1}")
print(f"time q2 = {toc_q2-tic_q2}")
print(f"time q3 = {toc_q3-tic_q3}")
print(f"time q4 = {toc_q4-tic_q4}")
print(f"time q5 = {toc_q5-tic_q5}")
print(f"time q7 = {toc_q7-tic_q7}")
print(f"time q8 = {toc_q8-tic_q8}")
print(f"time q9 = {toc_q9-tic_q9}")


client.close()
cluster.close()
