import os
import coiled
import pytest


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


@pytest.fixture
def tpch_dataset_name():
    return ENABLED_DATASET

@pytest.fixture
def tpch_dataset_path(tpch_dataset_name):
    return DATASETS[tpch_dataset_name]

@pytest.fixture
def vm_type():
    return 'm6i.16xlarge'

@pytest.fixture
def region():
    # Region of the TPCH data
    return 'us-east-2'

@pytest.fixture
def coiled_function(vm_type, region):
    # Dask single VM will use LocalCluster(processes=True)
    import dask
    with dask.config.set({"distributed.worker.daemon": False}):
        yield coiled.function(vm_type=vm_type, region=region)
