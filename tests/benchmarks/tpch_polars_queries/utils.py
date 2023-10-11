from pyarrow.dataset import dataset
from s3fs import S3FileSystem
import polars as pl
from tests.benchmarks.conftest import ENABLED_DATASET, DATASETS


def read_data(name, source=None):
    pyarrow_dataset = dataset(
        source=source or f"{DATASETS[ENABLED_DATASET]}{name}/",
        format="parquet"
    )
    return pl.scan_pyarrow_dataset(pyarrow_dataset)
