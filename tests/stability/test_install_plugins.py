import os

import pytest
from distributed import PipInstall

from ..utils_test import wait


@pytest.mark.parametrize("restart_workers", [True, False])
def test_private_pip_install(small_client, restart_workers):
    small_client.cluster.send_private_envs(
        {"PYTHON_STUB_TOKEN": os.environ["PYTHON_STUB_PAT"]}
    )

    plugin = PipInstall(
        packages=[
            "python_stub@git+https://${PYTHON_STUB_TOKEN}@github.com/coiled/python-stub.git"
        ],
        restart_workers=restart_workers,
    )
    small_client.register_plugin(plugin)

    def test(x):
        from python_stub import stub

        return stub.echo(x)

    fut = small_client.submit(test, "Hello, world!")
    wait(fut, small_client, 5 * 60)
