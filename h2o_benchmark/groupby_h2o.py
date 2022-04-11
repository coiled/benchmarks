"""
h2o-ai benchmark groupby part running on coiled.
"""
import datetime
import os
import timeit

import coiled
import dask.dataframe as dd
import pandas
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


q1(ddf)
q1_stats = client.sync(client.scheduler.get_task_groups)

df_q1 = pandas.DataFrame.from_dict(q1_stats, orient="index")
df_q1.index.name = "group"

print("q1 STATS")
print(df_q1)

client.restart()

q2(ddf)
q2_stats = client.sync(client.scheduler.get_task_groups)

df_q2 = pandas.DataFrame.from_dict(q2_stats, orient="index")
df_q2.index.name = "group"

print("q2 STATS")
print(df_q2)

# save this times somewhere, maybe for now print them.
print(f"{time_cluster= }")

client.close()
cluster.close()
