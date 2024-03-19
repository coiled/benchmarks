from __future__ import annotations

import glob
import json
import os.path
from collections import defaultdict
from typing import TypedDict

import yaml


class JSONOutput(TypedDict):
    run_AB: bool
    repeat: list[int]
    runtime: list[str]
    max_parallel: int
    pytest_args: list[str]
    h2o_datasets: list[str]


DO_NOT_RUN: JSONOutput = {
    "run_AB": False,
    "repeat": [],
    "runtime": [],
    "max_parallel": 1,
    "pytest_args": [],
    "h2o_datasets": [],
}

SUFFIXES = {
    "cluster.yaml",
    "conda.yaml",
    "dask.yaml",
    "requirements.in",
}


def discover_runtimes() -> list[str]:
    found_files = defaultdict(set)
    for suffix in SUFFIXES:
        for fname in glob.glob(f"AB_environments/AB_*.{suffix}"):
            runtime, _, _ = os.path.basename(fname).partition(".")
            found_files[runtime].add(suffix)

    missing_files = set()
    for runtime, found_suffixes in found_files.items():
        missing_files.update(
            f"AB_environments/{runtime}.{suffix}"
            for suffix in (SUFFIXES - found_suffixes)
        )
    if missing_files:
        raise FileNotFoundError(
            f"Incomplete A/B files set(s); file(s) not found: {sorted(missing_files)}"
        )
    return sorted(found_files)


def build_json() -> JSONOutput:
    with open("AB_environments/config.yaml") as fh:
        cfg = yaml.safe_load(fh)

    if not isinstance(cfg.get("repeat"), int) or cfg["repeat"] < 0:
        raise ValueError("AB_environments/config.yaml: missing key {repeat: N}")
    for target in cfg["targets"]:
        target = target.split("::")[0]
        if not os.path.exists(target):
            raise FileNotFoundError(target)

    if not cfg["repeat"] or not cfg["targets"]:
        return DO_NOT_RUN

    runtimes = discover_runtimes()

    if not runtimes:
        return DO_NOT_RUN

    if "AB_baseline" not in runtimes:
        # If any A/B environments are defined, AB_baseline is required
        raise FileNotFoundError("AB_environments/AB_baseline.conda.yaml")

    if cfg["test_null_hypothesis"]:
        runtimes += ["AB_null_hypothesis"]

    pytest_args = []
    if (n := cfg["max_parallel"]["pytest_workers_per_job"]) > 1:
        pytest_args.append(f"-n {n} --dist loadscope")
    if cfg["markers"]:
        pytest_args.append(f"-m '{cfg['markers']}'")
    for target in cfg["targets"]:
        pytest_args.append(f"'{target}'")

    return {
        "run_AB": True,
        "repeat": list(range(1, cfg["repeat"] + 1)),
        "runtime": runtimes,
        "max_parallel": cfg["max_parallel"]["ci_jobs"],
        "pytest_args": [" ".join(pytest_args)],
        "h2o_datasets": [",".join(cfg["h2o_datasets"])],
    }


def main() -> None:
    print(json.dumps(build_json()))


if __name__ == "__main__":
    main()
