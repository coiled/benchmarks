"""
h2o-ai benchmark groupby part running on coiled.
"""
import datetime
import logging
import os
import uuid

import coiled
import dask.dataframe as dd
import pandas as pd
import pytest
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


@pytest.fixture(scope="module")
def cluster():
    with coiled.Cluster(
        software=SOFTWARE,
        name="h2o-groupby-benchmark_" + str(uuid.uuid4()),
        account="dask-engineering",
        n_workers=4,
        backend_options={"spot": False},
    ) as cluster:
        yield cluster


plugin = TaskGroupStatistics()


@pytest.fixture()
def client(cluster):
    with Client(cluster) as client:
        client.register_scheduler_plugin(plugin)
        yield client
        client.restart()


@pytest.fixture(scope="module")
def ddf():
    yield dd.read_csv(
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


logger = logging.getLogger("h2o")


@pytest.fixture(autouse=True)
def print_stats(client):
    try:
        yield
    finally:
        stats = client.sync(client.scheduler.get_task_groups)
        df = pd.DataFrame.from_dict(stats, orient="index")
        df.index.name = "group"
        logger.info("\n" + df.to_string(index=False))


def test_q1(ddf):
    return ddf.groupby("id1", dropna=False, observed=True).agg({"v1": "sum"}).compute()


def test_q2(ddf):
    return (
        ddf.groupby(["id1", "id2"], dropna=False, observed=True)
        .agg({"v1": "sum"})
        .compute()
    )


def test_q3(ddf):
    return (
        ddf.groupby("id3", dropna=False, observed=True)
        .agg({"v1": "sum", "v3": "mean"})
        .compute()
    )


def test_q4(ddf):
    return (
        ddf.groupby("id4", dropna=False, observed=True)
        .agg({"v1": "mean", "v2": "mean", "v3": "mean"})
        .compute()
    )


def test_q5(ddf):
    return (
        ddf.groupby("id6", dropna=False, observed=True)
        .agg({"v1": "sum", "v2": "sum", "v3": "sum"})
        .compute()
    )


def test_q7(ddf):
    return (
        ddf.groupby("id3", dropna=False, observed=True)
        .agg({"v1": "max", "v2": "min"})
        .assign(range_v1_v2=lambda x: x["v1"] - x["v2"])[["range_v1_v2"]]
        .compute()
    )


def test_q8(ddf):
    return (
        ddf[~ddf["v3"].isna()][["id6", "v3"]]
        .groupby("id6", dropna=False, observed=True)
        .apply(
            lambda x: x.nlargest(2, columns="v3"),
            meta={"id6": "Int64", "v3": "float64"},
        )[["v3"]]
        .compute()
    )


def test_q9(ddf):
    return (
        ddf[["id2", "id4", "v1", "v2"]]
        .groupby(["id2", "id4"], dropna=False, observed=True)
        .apply(
            lambda x: pd.Series({"r2": x.corr()["v1"]["v2"] ** 2}),
            meta={"r2": "float64"},
        )
        .compute()
    )
