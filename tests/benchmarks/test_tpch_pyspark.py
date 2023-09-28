import asyncio
import os
import pathlib
import shlex
import subprocess
import sys

import pyspark
import pytest
from conda.cli.python_api import Commands, run_command
from distributed.diagnostics.plugin import SchedulerPlugin, WorkerPlugin

from . import tpch_pyspark_queries as queries

# pyspark/hadoop/aws-java-* are extra sensitive to version changes it seems.
# ref: https://hadoop.apache.org/docs/r3.3.4/hadoop-aws/dependency-analysis.html
# for verifying aws-java-sdk-bundle version w/ hadoop version.
assert pyspark.__version__ == "3.4.1"
HADOOP_AWS_VERSION = "3.3.4"  # sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
AWS_JAVA_SDK_BUNDLE_VERSION = "1.12.262"

DATASETS = {
    "scale 100": "s3a://coiled-runtime-ci/tpch_scale_100/",
    "scale 1000": "s3a://coiled-runtime-ci/tpch_scale_1000/",
}


ENABLED_DATASET = os.getenv("TPCH_SCALE")
if ENABLED_DATASET is not None:
    if ENABLED_DATASET not in DATASETS:
        raise ValueError("Unknown tpch dataset: ", ENABLED_DATASET)
else:
    ENABLED_DATASET = "scale 100"


@pytest.mark.parametrize(
    "name",
    sorted(
        [name for name in dir(queries) if name.startswith("q")],
        key=lambda v: int(v.replace("q", "")),
    ),
)
def test_tpch_pyspark(tpch_pyspark_client, name):
    from .tpch_pyspark_queries.utils import get_or_create_spark

    module = getattr(queries, name)

    async def run_tpch(dask_scheduler):
        # Connecting to the Spark Connect server doens't appear to work very well
        # If we just submit this stuff to the scheduler and generate a SparkSession there
        # that connects to the running master we can at least get some code running
        def _():
            # TODO: Separate setup queries and actual benchmark query timings?
            module.setup()  # read spark tables query will select from

            if hasattr(module, "ddl"):
                get_or_create_spark().sql(module.ddl)  # might create temp view

            q_final = get_or_create_spark().sql(module.query)  # benchmark query
            print(q_final)

        return await asyncio.to_thread(_)

    tpch_pyspark_client.run_on_scheduler(run_tpch)


def download_jars():
    subprocess.check_output(
        [
            "curl",
            "--output",
            f"/tmp/hadoop-aws-{HADOOP_AWS_VERSION}.jar",
            f"https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/{HADOOP_AWS_VERSION}/hadoop-aws-{HADOOP_AWS_VERSION}.jar",  # noqa: E501
        ]
    )
    subprocess.check_output(
        [
            "curl",
            "--output",
            f"/tmp/aws-java-sdk-bundle-{AWS_JAVA_SDK_BUNDLE_VERSION}.jar",
            f"https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/{AWS_JAVA_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-{AWS_JAVA_SDK_BUNDLE_VERSION}.jar",  # noqa: E501
        ]
    )


class SparkMaster(SchedulerPlugin):
    def start(self, scheduler):
        self.scheduler = scheduler
        self.scheduler.add_plugin(self)

        print("Downloading jars..")
        download_jars()
        print("Done downloading jars.")

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
        path = pathlib.Path(pyspark.__file__)
        module_loc = path.parent
        script = module_loc / "sbin" / "spark-daemon.sh"  # noqa: F841

        os.environ["SPARK_HOME"] = str(module_loc)
        os.environ["PYSPARK_PYTHON"] = sys.executable

        cmd = "spark-class org.apache.spark.deploy.master.Master"
        print(f"Executing\n{cmd}")
        self.proc_master = subprocess.Popen(shlex.split(cmd))

        print("Launched Spark Master")

    def close(self, worker):
        self.proc_master.terminate()
        self.proc_master.wait()
        return super().close(worker)


class SparkConnect(SchedulerPlugin):
    def start(self, scheduler):
        print("Starting SparkConnect")
        self.scheduler = scheduler
        self.scheduler.add_plugin(self)
        path = pathlib.Path(pyspark.__file__)
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
            --packages org.apache.hadoop:hadoop-aws::{HADOOP_AWS_VERSION} \
            --packages com.amazonaws:aws-java-sdk-bundle:{AWS_JAVA_SDK_BUNDLE_VERSION} \
            --master=spark://{spark_master} \
            --conf spark.driver.host={host} \
            --conf spark.driver.port=9797 \
            --conf spark.jars=/tmp/hadoop-aws-{HADOOP_AWS_VERSION}.jar,/tmp/aws-java-sdk-bundle-{AWS_JAVA_SDK_BUNDLE_VERSION}.jar
            """  # noqa: E501
        print(f"Executing\n{cmd}")
        self.proc_connect = subprocess.Popen(shlex.split(cmd))

    def close(self, worker):
        self.proc_connect.terminate()
        self.proc_connect.wait()
        return super().close(worker)


class SparkWorker(WorkerPlugin):
    def setup(self, worker):
        self.worker = worker

        print("Downloading jars..")
        download_jars()
        print("Done downloading jars.")

        path = pathlib.Path(pyspark.__file__)
        module_loc = path.parent

        os.environ["SPARK_HOME"] = str(module_loc)
        os.environ["PYSPARK_PYTHON"] = sys.executable

        host = worker.scheduler.address.split("//")[1].split(":")[0]
        spark_master = f"{host}:7077"

        print(f"Launching Spark Worker connecting to {spark_master}")
        cmd = f"spark-class org.apache.spark.deploy.worker.Worker {spark_master}"
        self.proc = subprocess.Popen(shlex.split(cmd))
        print("Launched Spark Worker")

    def close(self, worker):
        self.proc.terminate()
        self.proc.wait()
        return super().close(worker)
