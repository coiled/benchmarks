import functools


def get_or_create_spark():
    from distributed.utils import get_ip
    from pyspark.sql import SparkSession

    from ..test_tpch_pyspark import AWS_JAVA_SDK_BUNDLE_VERSION, HADOOP_AWS_VERSION

    return (
        SparkSession.builder.master(f"spark://{get_ip()}:7077")
        .appName("SparkTest")
        .config(
            "spark.jars",
            f"/tmp/hadoop-aws-{HADOOP_AWS_VERSION}.jar,/tmp/aws-java-sdk-bundle-{AWS_JAVA_SDK_BUNDLE_VERSION}.jar",
        )
        # ref: https://issues.apache.org/jira/browse/SPARK-44988  (unresolved, v3.4.0 and v3.4.1 affected)
        # Illegal Parquet type: INT64 (TIMESTAMP(NANOS,true))
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .config(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
        .config("spark.sql.shuffle.partitions", "400")
        .config("spark.memory.fraction", "0.7")
        .getOrCreate()
    )


def drop_temp_views():
    spark = get_or_create_spark()
    [
        spark.catalog.dropTempView(t.name)
        for t in spark.catalog.listTables()
        if t.isTemporary
    ]


@functools.lru_cache
def read_parquet_spark(filename: str, table_name: str):
    from ..test_tpch_pyspark import DATASETS, ENABLED_DATASET

    path = DATASETS[ENABLED_DATASET] + filename + "/"
    print(f"Read from {path=}")
    df = get_or_create_spark().read.parquet(path)
    df.createOrReplaceTempView(table_name)
    return df
