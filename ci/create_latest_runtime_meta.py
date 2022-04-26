from __future__ import annotations

import pathlib
import sys

import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape


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

    # File compatible with `mamba install --file <...>`
    with open("latest.txt", "w") as f:
        f.write("\n".join(requirements))

    # File compatible with `mamba env create --file <...>`
    env = {"channels": ["conda-forge"]}
    env["dependencies"] = requirements
    with open("latest.yaml", "w") as f:
        yaml.dump(env, f)


if __name__ == "__main__":
    main()
