import asyncio
import logging
import os
import pathlib
import re
import shlex
import subprocess
import sys

import botocore
import duckdb
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
    "local": "./tpch-data/scale10/",
    "scale 100": "s3://coiled-runtime-ci/tpch_scale_100/",
    "scale 1000": "s3://coiled-runtime-ci/tpch-scale-1000/",
}

ENABLED_DATASET = os.getenv("TPCH_SCALE")
if ENABLED_DATASET is not None:
    if ENABLED_DATASET not in DATASETS:
        raise ValueError("Unknown tpch dataset: ", ENABLED_DATASET)
else:
    ENABLED_DATASET = "scale 100"


logger = logging.getLogger("coiled")

PACKAGES = (
    f"org.apache.hadoop:hadoop-client:{HADOOP_AWS_VERSION}",
    f"org.apache.hadoop:hadoop-common:{HADOOP_AWS_VERSION}",
    f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
    f"com.amazonaws:aws-java-sdk-bundle:{AWS_JAVA_SDK_BUNDLE_VERSION}",
)


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
        from distributed.utils import get_ip

        spark_master = f"{get_ip()}:7077"
        os.environ["SPARK_HOME"] = str(pathlib.Path(pyspark.__file__).absolute().parent)
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_SUBMIT_ARGS"] = make_pyspark_submit_args(spark_master)

        def _():
            spark = get_or_create_spark(f"query{name}")
            module.setup(spark)  # read spark tables query will select from

            if hasattr(module, "ddl"):
                spark.sql(module.ddl)  # might create temp view

            # scale1000 stored as timestamp[ns] which spark parquet
            # can't use natively.
            if ENABLED_DATASET == "scale 1000":
                module.query = fix_timestamp_ns_columns(module.query)

            q_final = spark.sql(module.query)  # benchmark query
            try:
                # trigger materialization of df
                return q_final.toJSON().collect()
            finally:
                spark.catalog.clearCache()
                spark.sparkContext.stop()
                spark.stop()

        return await asyncio.to_thread(_)

    rows = tpch_pyspark_client.run_on_scheduler(_run_tpch)
    print(f"Received {len(rows)} rows")


class TestTpchCluster:
    """
    TPCH query benchmarks for Dask vs PySpark

    Tests parametrized queries by dask, pyspark; taken from implementations
    in test_tpch (dask) and test_tpch_pyspark (pyspark).
    """

    @classmethod
    def generate_test_cases(cls):
        from . import test_tpch

        def make_test(dask, pyspark, query_num):
            @pytest.mark.parametrize("engine", ("dask", "pyspark"))
            def func(_self, tpch_pyspark_client, engine):
                if engine == "dask":
                    dask(tpch_pyspark_client)
                else:
                    pyspark(tpch_pyspark_client, f"q{query_num}")

            func.__name__ = f"test_query_{query_num}"
            return func

        for query_num in range(1, 23):
            test_tpch_dask = getattr(test_tpch, f"test_query_{query_num}", None)
            if test_tpch_dask is not None:
                test = make_test(test_tpch_dask, test_tpch_pyspark, query_num)
                setattr(cls, test.__name__, test)


TestTpchCluster.generate_test_cases()


class TestTpchSingleVM:
    @classmethod
    def generate_test_cases(cls):
        import coiled

        from . import test_tpch
        from . import tpch_duckdb_queries as duckdb_queries

        def make_test(dask, pyspark, query_num):
            @pytest.mark.parametrize("engine", ("duckdb", "dask", "pyspark"))
            def func(_self, engine):
                @coiled.function(vm_type="m6i.12xlarge", region="us-east-2")
                def _():
                    if engine == "duckdb":
                        module = getattr(duckdb_queries, f"q{query_num}")

                        con = duckdb.connect()

                        if ENABLED_DATASET != "local":  # Setup s3 credentials
                            creds = botocore.session.get_session().get_credentials()
                            con.install_extension("httpfs")
                            con.load_extension("httpfs")
                            con.sql(
                                f"""
                                SET s3_region='us-east-2';
                                SET s3_access_key_id='{creds.access_key}';
                                SET s3_secret_access_key='{creds.secret_key}';
                                """
                            )
                        if hasattr(module, "ddl"):
                            con.sql(module.ddl)
                        _ = con.execute(module.query).fetch_arrow_table()

                    elif engine == "dask":
                        dask(None)  # No client needed/used by dask queries

                    elif engine == "pyspark":
                        import pyspark as pyspark_

                        from .tpch_pyspark_queries.utils import get_or_create_spark

                        queries.utils.MASTER = "local[*]"

                        # TODO: context env vars
                        os.environ["SPARK_HOME"] = str(
                            pathlib.Path(pyspark_.__file__).absolute().parent
                        )
                        os.environ["PYSPARK_PYTHON"] = sys.executable
                        os.environ["PYSPARK_SUBMIT_ARGS"] = make_pyspark_submit_args(
                            queries.utils.MASTER
                        )

                        spark = get_or_create_spark(f"query{query_num}")

                        module = getattr(queries, f"q{query_num}")
                        module.setup(spark)  # read spark tables query will select from

                        if hasattr(module, "ddl"):
                            spark.sql(module.ddl)  # might create temp view

                        # scale1000 stored as timestamp[ns] which spark parquet
                        # can't use natively.
                        if ENABLED_DATASET == "scale 1000":
                            module.query = fix_timestamp_ns_columns(module.query)

                        q_final = spark.sql(module.query)  # benchmark query
                        try:
                            # trigger materialization of df
                            _ = q_final.collect()
                        finally:
                            spark.catalog.clearCache()
                            spark.sparkContext.stop()
                            spark.stop()

                _.cluster.adapt(minimum=1, maximum=1)
                _()

            func.__name__ = f"test_query_{query_num}"
            return func

        for query_num in range(1, 23):
            # Limit to queries implemented in dask
            test_tpch_dask = getattr(test_tpch, f"test_query_{query_num}", None)
            if test_tpch_dask is not None:
                test = make_test(test_tpch_dask, test_tpch_pyspark, query_num)
                setattr(cls, test.__name__, test)


TestTpchSingleVM.generate_test_cases()


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
