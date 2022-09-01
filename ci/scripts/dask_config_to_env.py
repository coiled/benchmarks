#!/usr/bin/env python
"""Read a dask config file and print it out in the format `-e ENV=VALUE ENV=VALUE ...`
This script is a work-around to not being able to upload dask config files to
`conda env create`.
"""
from __future__ import annotations

import sys
from typing import Iterator

import yaml


def main(fname: str) -> None:
    with open(fname) as fh:
        cfg = yaml.safe_load(fh)
    # Print nothing in case of empty file, comments only, or empty dict
    if cfg:
        print("-e " + " ".join(traverse(cfg, [])))


def traverse(node: dict | list | str | float | None, path: list[str]) -> Iterator[str]:
    if isinstance(node, dict):
        for k, v in node.items():
            k = k.upper().replace("-", "_")
            yield from traverse(v, path + [k])
        return

    if not path:
        raise ValueError("The top-level element must be a dict")
    if isinstance(node, str) and "'" in node:
        raise ValueError("Unsupported character: ' (single quote)")

    yield "'DASK_" + "__".join(path) + f"={node}'"


if __name__ == "__main__":
    main(sys.argv[1])
