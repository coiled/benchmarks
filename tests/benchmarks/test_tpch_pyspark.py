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
    "scale 1000": "s3a://coiled-runtime-ci/tpch-scale-1000/",
}


ENABLED_DATASET = os.getenv("TPCH_SCALE")
if ENABLED_DATASET is not None:
    if ENABLED_DATASET not in DATASETS:
        raise ValueError("Unknown tpch dataset: ", ENABLED_DATASET)
else:
    ENABLED_DATASET = "scale 1000"


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

    async def _run_tpch(dask_scheduler):
        # Connecting to the Spark Connect server doens't appear to work very well
        # If we just submit this stuff to the scheduler and generate a SparkSession there
        # that connects to the running master we can at least get some code running
        def _():
            module.setup()  # read spark tables query will select from

            if hasattr(module, "ddl"):
                get_or_create_spark().sql(module.ddl)  # might create temp view

            q_final = get_or_create_spark().sql(module.query)  # benchmark query

            # Sporadic Forbidden 403 error from S3 here.
            n_tries = 5
            for i in range(n_tries):
                try:
                    print(q_final.show(10))  # trigger materialization of df

                # Unfortunately, just the `Exception` class used from pyspark for the S3 403 err
                except Exception:
                    if i < n_tries - 1:
                        import time

                        time.sleep(5)
                        continue
                    raise
                else:
                    break

        return await asyncio.to_thread(_)

    tpch_pyspark_client.run_on_scheduler(_run_tpch)


def generate_dask_vs_pyspark_cases():
    from . import test_tpch

    for i in range(1, 22):
        test_tpch_dask = getattr(test_tpch, f"test_query_{i}", None)
        if test_tpch_dask is not None:

            def make_test(dask, pyspark):
                @pytest.mark.parametrize("engine", ("dask", "pyspark"))
                def func(tpch_pyspark_client, engine):
                    if engine == "dask":
                        dask(tpch_pyspark_client)
                    else:
                        pyspark(tpch_pyspark_client, f"q{i}")

                func.__name__ = f"test_tpch_dask_vs_pyspark_query_{i}"
                return func

            test = make_test(test_tpch_dask, test_tpch_pyspark)
            globals()[test.__name__] = test


generate_dask_vs_pyspark_cases()


def download_jars():
    hadoop_aws = pathlib.Path(f"/tmp/hadoop-aws-{HADOOP_AWS_VERSION}.jar")
    if not hadoop_aws.exists():
        print("Hadoop AWS JAR not found, downloading...")
        subprocess.check_output(
            [
                "curl",
                "--output",
                str(hadoop_aws),
                f"https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/{HADOOP_AWS_VERSION}/hadoop-aws-{HADOOP_AWS_VERSION}.jar",  # noqa: E501
            ]
        )
        print("done.")

    aws_sdk = pathlib.Path(
        f"/tmp/aws-java-sdk-bundle-{AWS_JAVA_SDK_BUNDLE_VERSION}.jar"
    )
    if not aws_sdk.exists():
        print("AWS JAVA SDK not found, downloading...")
        subprocess.check_output(
            [
                "curl",
                "--output",
                str(aws_sdk),
                f"https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/{AWS_JAVA_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-{AWS_JAVA_SDK_BUNDLE_VERSION}.jar",  # noqa: E501
            ]
        )
        print("done.")


class SparkMaster(SchedulerPlugin):
    def start(self, scheduler):
        self.scheduler = scheduler
        self.scheduler.add_plugin(self)

        download_jars()

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

    def close(self):
        self.proc_master.terminate()
        self.proc_master.wait()
        return super().close()


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

    def close(self):
        self.proc_connect.terminate()
        self.proc_connect.wait()
        return super().close()


class SparkWorker(WorkerPlugin):
    def setup(self, worker):
        self.worker = worker

        download_jars()

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

    def teardown(self, worker):
        self.proc.terminate()
        self.proc.wait()
        return super().teardown(worker)
