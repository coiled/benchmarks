import os
import sys

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-latest", action="store_true", help="Run latest coiled-runtime tests"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-latest"):
        # --run-latest given in cli: do not skip latest coiled-runtime tests
        return
    skip_latest = pytest.mark.skip(reason="need --run-latest option to run")
    for item in items:
        if "latest_runtime" in item.keywords:
            item.add_marker(skip_latest)


@pytest.fixture(scope="session")
def runtime_software_env():
    return os.environ.get(
        "COILED_SOFTWARE_NAME",
        f"dask-engineering/coiled_dist-py{sys.version_info[0]}{sys.version_info[1]}",
    )
