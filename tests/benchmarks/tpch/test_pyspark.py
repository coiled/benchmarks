import asyncio
import logging
import os
import pathlib
import re
import shlex
import subprocess
import sys

import pytest
from conda.cli.python_api import Commands, run_command
from distributed.diagnostics.plugin import SchedulerPlugin, WorkerPlugin
from distributed.utils import get_ip

from tests.benchmarks.tpch.conftest import DATASETS, ENABLED_DATASET  # noqa: F401

from . import pyspark_queries as queries

pyspark = pytest.importorskip("pyspark")


# pyspark/hadoop/aws-java-* are extra sensitive to version changes it seems.
# ref: https://hadoop.apache.org/docs/r3.3.4/hadoop-aws/dependency-analysis.html
# for verifying aws-java-sdk-bundle version w/ hadoop version.
assert pyspark.__version__ == "3.4.1"
HADOOP_AWS_VERSION = "3.3.4"  # sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
AWS_JAVA_SDK_BUNDLE_VERSION = "1.12.262"


logger = logging.getLogger("coiled")

PACKAGES = (
    f"org.apache.hadoop:hadoop-client:{HADOOP_AWS_VERSION}",
    f"org.apache.hadoop:hadoop-common:{HADOOP_AWS_VERSION}",
    f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
    f"com.amazonaws:aws-java-sdk-bundle:{AWS_JAVA_SDK_BUNDLE_VERSION}",
)


def test_query_1(tpch_pyspark_client):
    return run_tpch_pyspark(tpch_pyspark_client, queries.q1)


def test_query_2(tpch_pyspark_client):
    return run_tpch_pyspark(tpch_pyspark_client, queries.q2)


def test_query_3(tpch_pyspark_client):
    return run_tpch_pyspark(tpch_pyspark_client, queries.q3)


def test_query_4(tpch_pyspark_client):
    return run_tpch_pyspark(tpch_pyspark_client, queries.q4)


def test_query_5(tpch_pyspark_client):
    return run_tpch_pyspark(tpch_pyspark_client, queries.q5)


def test_query_6(tpch_pyspark_client):
    return run_tpch_pyspark(tpch_pyspark_client, queries.q6)


def test_query_7(tpch_pyspark_client):
    return run_tpch_pyspark(tpch_pyspark_client, queries.q7)


def make_pyspark_submit_args(spark_master):
    return f"""
        --packages {','.join(PACKAGES)}
        --exclude-packages com.google.guava:guava
        --master=spark://{spark_master}
        pyspark-shell
    """


def fix_timestamp_ns_columns(query):
    """
    scale100 stores l_shipdate/o_orderdate as timestamp[us]
    scale1000 stores l_shipdate/o_orderdate as timestamp[ns] which gives:
        Illegal Parquet type: INT64 (TIMESTAMP(NANOS,true))
    so we set spark.sql.legacy.parquet.nanosAsLong and then convert to timestamp.
    """
    for name in ("l_shipdate", "o_orderdate"):
        query = re.sub(rf"\b{name}\b", f"to_timestamp(cast({name} as string))", query)
    return query


def run_tpch_pyspark(tpch_pyspark_client, module):
    from .pyspark_queries.utils import get_or_create_spark

    is_local = tpch_pyspark_client is None

    async def _run_tpch(dask_scheduler):
        # Connecting to the Spark Connect server doens't appear to work very well
        # If we just submit this stuff to the scheduler and generate a SparkSession there
        # that connects to the running master we can at least get some code running

        if not is_local:
            queries.utils.MASTER = f"spark://{get_ip()}:7077"
        else:
            queries.utils.MASTER = "local[*]"

        os.environ["SPARK_HOME"] = str(pathlib.Path(pyspark.__file__).absolute().parent)
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_SUBMIT_ARGS"] = make_pyspark_submit_args(
            queries.utils.MASTER
        )

        def _():
            spark = get_or_create_spark(f"query{module.__name__}")

            # scale1000 stored as timestamp[ns] which spark parquet
            # can't use natively.
            if ENABLED_DATASET == "scale 1000":
                module.query = fix_timestamp_ns_columns(module.query)

            module.setup(spark)  # read spark tables query will select from
            if hasattr(module, "ddl"):
                spark.sql(module.ddl)  # might create temp view
            q_final = spark.sql(module.query)  # benchmark query
            try:
                # trigger materialization of df
                return q_final.toJSON().collect()
            finally:
                spark.catalog.clearCache()
                spark.sparkContext.stop()
                spark.stop()

        return await asyncio.to_thread(_)

    if not is_local:
        rows = tpch_pyspark_client.run_on_scheduler(_run_tpch)
    else:
        rows = asyncio.run(_run_tpch(None))  # running locally
    print(f"Received {len(rows)} rows")


class SparkMaster(SchedulerPlugin):
    def start(self, scheduler):
        self.scheduler = scheduler
        self.scheduler.add_plugin(self)

        print("Installing procps...", end="")
        try:
            # All those nasty scripts require ``ps`` unix CLI util to be installed
            _, stderr, returncode = run_command(
                Commands.INSTALL, ["-c", "conda-forge", "procps-ng"]
            )
        except Exception as e:
            msg = "conda install failed"
            raise RuntimeError(msg) from e
        print("done")
        path = pathlib.Path(pyspark.__file__).absolute()
        module_loc = path.parent
        script = module_loc / "sbin" / "spark-daemon.sh"  # noqa: F841

        host = scheduler.address.split("//")[1].split(":")[0]
        spark_master = f"{host}:7077"

        os.environ["SPARK_HOME"] = str(module_loc)
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_SUBMIT_ARGS"] = make_pyspark_submit_args(spark_master)

        cmd = "spark-class org.apache.spark.deploy.master.Master"
        print(f"Executing\n{cmd}")
        self.proc_master = subprocess.Popen(shlex.split(cmd))

        print("Launched Spark Master")

    def close(self):
        self.proc_master.terminate()
        self.proc_master.wait()
        return super().close()


class SparkConnect(SchedulerPlugin):
    def start(self, scheduler):
        print("Starting SparkConnect")
        self.scheduler = scheduler
        self.scheduler.add_plugin(self)
        path = pathlib.Path(pyspark.__file__).absolute()
        module_loc = path.parent
        script = module_loc / "sbin" / "spark-daemon.sh"

        os.environ["SPARK_HOME"] = str(module_loc)
        os.environ["PYSPARK_PYTHON"] = sys.executable

        class_ = "org.apache.spark.sql.connect.service.SparkConnectServer"
        host = scheduler.address.split("//")[1].split(":")[0]
        spark_master = f"{host}:7077"
        cmd = f"""{script} submit {class_} 1 \
            --name "Spark Connect server" \
            --packages org.apache.spark:spark-connect_2.12:{pyspark.__version__} \
            --packages org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION} \
            --packages com.amazonaws:aws-java-sdk-bundle:{AWS_JAVA_SDK_BUNDLE_VERSION} \
            --master=spark://{spark_master} \
            --conf spark.driver.host={host} \
            --conf spark.driver.port=9797 \
            --conf spark.jars=/tmp/hadoop-aws-{HADOOP_AWS_VERSION}.jar,/tmp/aws-java-sdk-bundle-{AWS_JAVA_SDK_BUNDLE_VERSION}.jar
            """  # noqa: E501
        print(f"Executing\n{cmd}")
        self.proc_connect = subprocess.Popen(shlex.split(cmd))

    def close(self):
        self.proc_connect.terminate()
        self.proc_connect.wait()
        return super().close()


class SparkWorker(WorkerPlugin):
    def setup(self, worker):
        self.worker = worker

        path = pathlib.Path(pyspark.__file__).absolute()
        module_loc = path.parent
        host = worker.scheduler.address.split("//")[1].split(":")[0]
        spark_master = f"{host}:7077"

        os.environ["SPARK_HOME"] = str(module_loc)
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_SUBMIT_ARGS"] = make_pyspark_submit_args(spark_master)

        print(f"Launching Spark Worker connecting to {spark_master}")
        cmd = f"spark-class org.apache.spark.deploy.worker.Worker {spark_master}"
        self.proc = subprocess.Popen(shlex.split(cmd))
        print("Launched Spark Worker")

    def teardown(self, worker):
        self.proc.terminate()
        self.proc.wait()
        return super().teardown(worker)
