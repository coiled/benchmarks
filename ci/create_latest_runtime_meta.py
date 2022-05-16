from __future__ import annotations

import json
import os
import pathlib
import shlex
import subprocess
import sys
from distutils.util import strtobool

import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape
from packaging.requirements import Requirement


def get_latest_conda_build(package):
    """Retrieve URL for most recent `package` version on the `dask/label/dev` conda channel"""
    cmd = f"conda search --json --override-channels -c dask/label/dev {package}"
    out = subprocess.check_output(shlex.split(cmd), text=True)
    result = json.loads(out)[package][-1]
    return result["url"]


def main():
    """Get package version pins specified in `recipe/meta.yaml`"""
    env = Environment(
        loader=FileSystemLoader(pathlib.Path(__file__).parent.parent / "recipe"),
        autoescape=select_autoescape(),
    )
    template = env.get_template("meta.yaml")
    meta = yaml.safe_load(template.render())
    requirements = meta["requirements"]["run"]

    # Ensure Python is pinned to X.Y.Z version currently being used
    python_version = ".".join(map(str, tuple(sys.version_info)[:3]))
    for idx, req in enumerate(requirements):
        package_name = Requirement(req).name
        if package_name == "python":
            requirements[idx] = f"python =={python_version}"

    # Optionally use the development version of `dask` and `distributed`
    # from `dask/label/dev` conda channel
    upstream = strtobool(os.environ.get("TEST_UPSTREAM", "false"))
    pre_release = strtobool(os.environ.get("PRE_RELEASE", "false"))
    if upstream or pre_release:
        upstream_packages = {"dask", "distributed"}
        for idx, req in enumerate(requirements):
            package_name = Requirement(req).name
            if package_name in upstream_packages:
                requirements[idx] = get_latest_conda_build(package_name)

    if pre_release:
        with open("pre_release.yaml", "w") as f:
            yaml.dump(meta, f)

    else:
        # File compatible with `mamba env create --file <...>`
        env = {
            "channels": ["conda-forge"],
            "dependencies": requirements,
        }
        with open("latest.yaml", "w") as f:
            yaml.dump(env, f)


if __name__ == "__main__":
    main()
