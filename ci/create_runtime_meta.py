from __future__ import annotations

import json
import os
import pathlib
import shlex
import subprocess
import sys

import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape
from packaging.requirements import InvalidRequirement, Requirement


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
    meta = yaml.safe_load(template.render(environ=os.environ))
    requirements = meta["requirements"]["run"]

    # Ensure Python is pinned to X.Y.Z version currently being used
    python_version = ".".join(map(str, tuple(sys.version_info)[:3]))
    for idx, req in enumerate(requirements):
        try:
            package_name = Requirement(req).name
        except InvalidRequirement:
            # The `g` at the end of the openssl version is not compatible with
            # the way Requirements is parsing versions starting with the latest
            # CPython patch releases (3.10.9, 3.9.16, 3.8.16)
            if "openssl" not in req:
                raise
            continue

        if package_name == "python":
            requirements[idx] = f"python =={python_version}"

    if os.environ.get("COILED_RUNTIME_VERSION", "unknown") == "upstream":
        requirements = [
            r
            for r in requirements
            if Requirement(r).name not in {"dask", "distributed"}
        ]
        requirements.append(
            {
                "pip": [
                    "git+https://github.com/dask/dask@main",
                    "git+https://github.com/dask/distributed@main",
                ]
            }
        )

    # File compatible with `mamba env create --file <...>`
    env = {
        "channels": ["conda-forge"],
        "dependencies": requirements,
    }
    with open("coiled_software_environment.yaml", "w") as f:
        yaml.dump(env, f)


if __name__ == "__main__":
    main()
