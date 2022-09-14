import glob
import json
import os.path


def main():
    envs = []
    for conda_fname in sorted(glob.glob("AB_environments/AB_*.conda.yaml")):
        env_name = os.path.basename(conda_fname)[: -len(".conda.yaml")]
        dask_fname = f"AB_environments/{env_name}.dask.yaml"
        # Raise FileNotFoundError if missing
        open(dask_fname).close()
        envs.append(env_name)

    if envs and "AB_baseline" not in envs:
        # If any A/B environments are defined, AB_baseline is required
        raise FileNotFoundError("AB_environments/AB_baseline.conda.yaml")

    print(json.dumps(envs))


if __name__ == "__main__":
    main()
