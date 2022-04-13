from __future__ import annotations

import pathlib

import click
import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape


@click.command(context_settings={"ignore_unknown_options": True})
@click.option(
    "--output",
    type=click.Path(dir_okay=False, writable=True, path_type=pathlib.Path),
    required=True,
    help="Output file",
)
def main(output):
    """Get package version pins specified in `recipe/meta.yaml`"""
    env = Environment(
        loader=FileSystemLoader(pathlib.Path(__file__).parent.parent / "recipe"),
        autoescape=select_autoescape(),
    )
    template = env.get_template("meta.yaml")
    meta = yaml.safe_load(template.render())
    with open(output, "w") as f:
        f.write("\n".join(meta["requirements"]["run"]))


if __name__ == "__main__":
    main()
