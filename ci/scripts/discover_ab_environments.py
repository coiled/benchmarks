from __future__ import annotations

import glob
import json
import os.path
from typing import TypedDict

import yaml


class JSONOutput(TypedDict):
    run_AB: bool
    repeat: list[int]
    runtime: list[str]
    pytest_args: list[str]


def build_json() -> JSONOutput:
    with open("AB_environments/config.yaml") as fh:
        cfg = yaml.safe_load(fh)

    if not isinstance(cfg.get("repeat"), int) or cfg["repeat"] < 0:
        raise ValueError("AB_environments/config.yaml: missing key {repeat: N}")
    for category in cfg["categories"]:
        if not glob.glob(f"tests/{category}/test_*.py"):
            raise ValueError("fNot a valid test category: {category}")

    if not cfg["repeat"] or not cfg["categories"]:
        return {"run_AB": False, "repeat": [], "runtime": [], "pytest_args": []}

    runtimes = []
    for conda_fname in sorted(glob.glob("AB_environments/AB_*.conda.yaml")):
        env_name = os.path.basename(conda_fname)[: -len(".conda.yaml")]
        dask_fname = f"AB_environments/{env_name}.dask.yaml"
        # Raise FileNotFoundError if missing
        open(dask_fname).close()
        runtimes.append(env_name)

    if not runtimes:
        return {"run_AB": False, "repeat": [], "runtime": [], "pytest_args": []}

    if "AB_baseline" not in runtimes:
        # If any A/B environments are defined, AB_baseline is required
        raise FileNotFoundError("AB_environments/AB_baseline.conda.yaml")

    if cfg["test_null_hypothesis"]:
        runtimes += ["AB_null_hypothesis"]

    return {
        "run_AB": True,
        "repeat": list(range(1, cfg["repeat"] + 1)),
        "runtime": runtimes,
        "pytest_args": [" ".join(f"tests/{c}" for c in cfg["categories"])],
    }


def main() -> None:
    print(json.dumps(build_json()))


if __name__ == "__main__":
    main()
