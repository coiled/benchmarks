from __future__ import annotations

import pathlib

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

    # File compatible with `mamba install --file <...>`
    with open("latest.txt", "w") as f:
        f.write("\n".join(requirements))

    # File compatible with `mamba env create --file <...>`
    env = {"channels": ["coiled", "conda-forge"]}
    env["dependencies"] = requirements
    with open("latest.yaml", "w") as f:
        yaml.dump(env, f)


if __name__ == "__main__":
    main()
