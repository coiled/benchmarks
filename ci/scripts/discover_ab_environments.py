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
    max_parallel: int
    pytest_args: list[str]


DO_NOT_RUN: JSONOutput = {
    "run_AB": False,
    "repeat": [],
    "runtime": [],
    "max_parallel": 1,
    "pytest_args": [],
}


def build_json() -> JSONOutput:
    with open("AB_environments/config.yaml") as fh:
        cfg = yaml.safe_load(fh)

    if not isinstance(cfg.get("repeat"), int) or cfg["repeat"] < 0:
        raise ValueError("AB_environments/config.yaml: missing key {repeat: N}")
    for category in cfg["categories"]:
        if not glob.glob(f"tests/{category}/test_*.py"):
            raise ValueError("fNot a valid test category: {category}")

    if not cfg["repeat"] or not cfg["categories"]:
        return DO_NOT_RUN

    runtimes = []
    for conda_fname in sorted(glob.glob("AB_environments/AB_*.conda.yaml")):
        env_name = os.path.basename(conda_fname)[: -len(".conda.yaml")]
        dask_fname = f"AB_environments/{env_name}.dask.yaml"
        # Raise FileNotFoundError if missing
        open(dask_fname).close()
        runtimes.append(env_name)

    if not runtimes:
        return DO_NOT_RUN

    if "AB_baseline" not in runtimes:
        # If any A/B environments are defined, AB_baseline is required
        raise FileNotFoundError("AB_environments/AB_baseline.conda.yaml")

    if cfg["test_null_hypothesis"]:
        runtimes += ["AB_null_hypothesis"]

    n = cfg["max_parallel"]["pytest_workers_per_job"]
    xdist_args = f"-n {n} --dist loadscope " if n > 1 else ""

    return {
        "run_AB": True,
        "repeat": list(range(1, cfg["repeat"] + 1)),
        "runtime": runtimes,
        "max_parallel": cfg["max_parallel"]["ci_jobs"],
        "pytest_args": [xdist_args + " ".join(f"tests/{c}" for c in cfg["categories"])],
    }


def main() -> None:
    print(json.dumps(build_json()))


if __name__ == "__main__":
    main()
