def get_or_create_spark(name: str = "SparkTest"):
    from distributed.utils import get_ip
    from pyspark.sql import SparkSession

    from ..test_tpch_pyspark import AWS_JAVA_SDK_BUNDLE_VERSION, HADOOP_AWS_VERSION

    return (
        SparkSession.builder.master(f"spark://{get_ip()}:7077")
        .appName(name)
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
        .config("spark.memory.fraction", "0.7")
        # TODO: Can these not be set automagically?!
        # ref: https://spark.apache.org/docs/latest/configuration.html#memory-management
        .config("spark.driver.cores", "4")  # defaults to 1
        .config("spark.driver.memory", "12g")  # defaults to 1GB
        .config("spark.executor.memory", "12g")  # defaults to 1GB
        .getOrCreate()
    )


def drop_temp_views():
    spark = get_or_create_spark()
    [
        spark.catalog.dropTempView(t.name)
        for t in spark.catalog.listTables()
        if t.isTemporary
    ]


def read_parquet_spark(spark, filename: str, table_name: str):
    from ..test_tpch_pyspark import DATASETS, ENABLED_DATASET

    path = DATASETS[ENABLED_DATASET] + filename + "/"
    print(f"Read from {path=}")
    df = spark.read.parquet(path)
    df.createOrReplaceTempView(table_name)
    return df
