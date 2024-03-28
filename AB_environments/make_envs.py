#!/usr/bin/env python
"""Simple utility to automate creation of A/B environment files"""
import argparse
import os
import shutil


def main():
    parser = argparse.ArgumentParser(
        description="Create A/B environment files as copies of AB_baseline"
    )
    parser.add_argument("name", nargs="+")
    names = parser.parse_args().name

    os.chdir(os.path.dirname(__file__))
    for name in names:
        if not name.startswith("AB_"):
            name = "AB_" + name
        for suffix in ("cluster.yaml", "conda.yaml", "dask.yaml", "requirements.in"):
            fname = f"{name}.{suffix}"
            if os.path.exists(fname):
                print(f"{fname} already exists")
            else:
                print(f"Creating {fname} as a copy of baseline")
                shutil.copy(f"AB_baseline.{suffix}", fname)


if __name__ == "__main__":
    main()
