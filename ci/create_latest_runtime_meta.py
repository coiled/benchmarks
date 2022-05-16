from __future__ import annotations

import json
import os
import pathlib
import shlex
import subprocess
import sys
from collections import OrderedDict
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
    pre_release = strtobool(os.environ.get("PRE_RELEASE", "false"))
    python_version = ".".join(map(str, tuple(sys.version_info)[:3]))
    for idx, req in enumerate(requirements):
        package_name = Requirement(req).name
        if package_name == "python" and not pre_release:
            requirements[idx] = f"python =={python_version}"

    # Optionally use the development version of `dask` and `distributed`
    # from `dask/label/dev` conda channel
    upstream = strtobool(os.environ.get("TEST_UPSTREAM", "false"))
    if upstream or pre_release:
        upstream_packages = {"dask", "distributed"}
        for idx, req in enumerate(requirements):
            package_name = Requirement(req).name
            if package_name in upstream_packages:
                requirements[idx] = get_latest_conda_build(package_name)

    if pre_release:
        from yaml import CDumper as Dumper

        def dict_representer(dumper, data):
            return dumper.represent_dict(data.items())

        Dumper.add_representer(OrderedDict, dict_representer)

        meta_ord = OrderedDict()

        meta_ord["package"] = meta["package"]
        meta_ord["source"] = meta["source"]
        meta_ord["build"] = meta["build"]
        meta_ord["requirement"] = meta["requirements"]
        meta_ord["test"] = meta["test"]
        meta_ord["about"] = meta["about"]
        meta_ord["extra"] = meta["extra"]

        meta_ord["package"]["version"] = os.environ.get("VERSION_SUFFIX")
        os.mkdir("pre_release")
        with open("pre_release/meta.yaml", "w") as f:
            yaml.dump(meta_ord, f, Dumper=Dumper)

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
