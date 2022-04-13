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
