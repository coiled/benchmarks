import psutil


def get_or_create_spark(name: str = "SparkTest"):
    from distributed.utils import get_ip
    from pyspark.sql import SparkSession

    # Memory for executors/driver - ~80% of total GiB
    memory = int((psutil.virtual_memory().total * 0.8) * 9.313226e-10)
    n_cores = psutil.cpu_count()

    spark = (
        SparkSession.builder.master(f"spark://{get_ip()}:7077")
        .appName(name)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
        # ref: https://issues.apache.org/jira/browse/SPARK-44988  (unresolved, v3.4.0 and v3.4.1 affected)
        # Illegal Parquet type: INT64 (TIMESTAMP(NANOS,true))
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .config("spark.memory.fraction", "0.7")
        # TODO: Can these not be set automagically by pyspark?!
        # ref: https://spark.apache.org/docs/latest/configuration.html#memory-management
        .config("spark.driver.cores", f"{n_cores}")  # defaults to 1
        .config("spark.driver.memory", f"{memory}g")  # defaults to 1GB
        .config("spark.executor.memory", f"{memory}g")  # defaults to 1GB
        .getOrCreate()
    )
    return spark


def read_parquet_spark(spark, filename: str, table_name: str):
    from ..test_tpch_pyspark import DATASETS, ENABLED_DATASET

    path = DATASETS[ENABLED_DATASET] + filename + "/"
    print(f"Read from {path=}")
    df = spark.read.parquet(path)
    df.createOrReplaceTempView(table_name)
    return df
