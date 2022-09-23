from __future__ import annotations

import glob
import json
import os.path

import yaml


def build_json() -> dict[str, list[int]]:
    with open("AB_environments/config.yaml") as fh:
        cfg = yaml.safe_load(fh)
    if not isinstance(cfg.get("repeat"), int) or cfg["repeat"] < 0:
        raise ValueError("AB_environments/config.yaml: missing key {repeat: N}")
    if not cfg["repeat"]:
        return {"run_AB": False, "repeat": [], "runtime": [], "category": []}

    runtimes = []
    for conda_fname in sorted(glob.glob("AB_environments/AB_*.conda.yaml")):
        env_name = os.path.basename(conda_fname)[: -len(".conda.yaml")]
        dask_fname = f"AB_environments/{env_name}.dask.yaml"
        # Raise FileNotFoundError if missing
        open(dask_fname).close()
        runtimes.append(env_name)

    if not runtimes:
        return {"run_AB": False, "repeat": [], "runtime": [], "category": []}

    if "AB_baseline" not in runtimes:
        # If any A/B environments are defined, AB_baseline is required
        raise FileNotFoundError("AB_environments/AB_baseline.conda.yaml")

    if cfg["test_null_hypothesis"]:
        runtimes += ["AB_null_hypothesis"]

    return {
        "run_AB": True,
        "repeat": list(range(1, cfg["repeat"] + 1)),
        "runtime": runtimes,
        "category": cfg["categories"],
    }


def main() -> None:
    print(json.dumps(build_json()))


if __name__ == "__main__":
    main()
