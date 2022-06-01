"""
Parquet-related benchmarks.
"""
import uuid

import dask.dataframe as dd
import distributed
import pandas
from coiled.v2 import Cluster


def test_read_spark_generated_data():
    """
    Read a ~15 GB subset of a ~800 GB spark-generated
    open dataset on AWS.
    """
    with Cluster(
        f"genome-{uuid.uuid4().hex[:8]}",
        n_workers=25,
        worker_vm_types=["m5.large"],
        scheduler_vm_types=["m5.large"],
    ) as cluster:
        with distributed.Client(cluster):
            ddf = dd.read_parquet(
                "s3://coiled-runtime-ci/thousandgenomes_dragen/var_partby_samples/NA21**.parquet",
                engine="pyarrow",
                index="sample_id",
            )
            ddf.groupby(ddf.index).first().compute()


def test_read_hive_partitioned_data():
    """
    Read a dataset partitioned by year and quarter.
    """
    with Cluster(
        f"ookla-{uuid.uuid4().hex[:8]}",
        n_workers=25,
        worker_vm_types=["m5.xlarge"],
        scheduler_vm_types=["m5.xlarge"],
    ) as cluster:
        with distributed.Client(cluster):
            ddf = dd.read_parquet(
                "s3://coiled-runtime-ci/ookla-open-data/type=fixed/**.parquet",
                engine="pyarrow",
            )

            # The data is already partitioned by year and quarter, but it doesn't
            # fit Dask's partitioning scheme well since we don't support multiindexes.
            # This is a lot of work to set the index for something that is already
            # partitioned! We also go around dask's set_index, which does a huge amount
            # of extra work, and even takes down instances.
            def get_period(df):
                return df.set_index(
                    pandas.PeriodIndex(
                        df.year.astype(str) + "Q" + df.quarter.astype(str),
                        freq="Q",
                        name="period",
                    )
                )

            ddf2 = ddf.map_partitions(get_period, meta=get_period(ddf._meta)).persist()
            distributed.wait(ddf2)
            # Enable once we have dask>=2022.3.0
            # ddf2.divisions = ddf2.compute_current_divisions()
