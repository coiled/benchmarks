from __future__ import annotations

import json
import shlex
import subprocess

import coiled
from packaging.version import Version


def test_latest_coiled():
    # Ensure that the conda environment installs the latest version of `coiled`
    # FIXME this test can glitch if you install coiled from pip
    v_installed = Version(coiled.__version__)

    # Get latest `coiled` release version from conda-forge
    output = subprocess.check_output(
        shlex.split("conda search --override-channels --json -c conda-forge coiled")
    )
    result = json.loads(output)
    v_latest = Version(result["coiled"][-1]["version"])
    # conda can lag behind a few days from pip; allow for the next version too
    v_allowed = {
        v_latest,
        Version(f"{v_latest.major}.{v_latest.minor}.{v_latest.micro + 1}"),
        Version(f"{v_latest.major}.{v_latest.minor + 1}.0"),
        Version(f"{v_latest.major + 1}.0.0"),
    }
    assert v_installed in v_allowed
