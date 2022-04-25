from __future__ import annotations

import json
import os
import pathlib
import sys
from urllib.request import urlopen

import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape


def get_latest_commit(repository):
    org, repo = repository.split("/")
    url = f"https://api.github.com/repos/{org}/{repo}/commits?per_page=1"
    response = urlopen(url).read()
    data = json.loads(response.decode())
    return data[0]["sha"]


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
    assert sum([req.startswith("python ") for req in requirements]) == 1

    for idx, req in enumerate(requirements):
        if req.startswith("python "):
            requirements[idx] = f"python =={python_version}"

    # Optionally install the development version of `dask` and `distributed`
    if os.environ.get("TEST_UPSTREAM", False):
        dev_req = {
            "pip": [
                f"git+https://github.com/dask/dask.git@{get_latest_commit('dask/dask')}",
                f"git+https://github.com/dask/distributed.git@{get_latest_commit('dask/distributed')}",
            ]
        }
        requirements.append(dev_req)

    # File compatible with `mamba env create --file <...>`
    env = {"channels": ["coiled", "conda-forge"]}
    env["dependencies"] = requirements
    with open("latest.yaml", "w") as f:
        yaml.dump(env, f)


if __name__ == "__main__":
    main()
