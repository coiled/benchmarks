from typing import Any

import boto3


def get_dataset_path(local, scale):
    remote_paths = {
        1: "s3://coiled-runtime-ci/tpc-h/snappy/scale-1/",
        10: "s3://coiled-runtime-ci/tpc-h/snappy/scale-10/",
        100: "s3://coiled-runtime-ci/tpc-h/snappy/scale-100/",
        1000: "s3://coiled-runtime-ci/tpc-h/snappy/scale-1000/",
        10000: "s3://coiled-runtime-ci/tpc-h/snappy/scale-10000/",
    }
    local_paths = {
        1: "./tpch-data/scale-1/",
        10: "./tpch-data/scale-10/",
        100: "./tpch-data/scale-100/",
    }

    if local:
        return local_paths[scale]
    else:
        return remote_paths[scale]


def get_answers_path(local, scale):
    if local:
        return f"./tpch-data/answers/scale-{scale}/"
    return f"s3://coiled-runtime-ci/tpc-h/answers/scale-{scale}/"


def get_bucket_region(path: str):
    if not path.startswith("s3://"):
        raise ValueError(f"'{path}' is not an S3 path")
    bucket = path.replace("s3://", "").split("/")[0]
    resp = boto3.client("s3").get_bucket_location(Bucket=bucket)
    # Buckets in region 'us-east-1' results in None, b/c why not.
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_bucket_location.html#S3.Client.get_bucket_location
    return resp["LocationConstraint"] or "us-east-1"


def get_cluster_spec(scale: int, shutdown_on_close: bool) -> dict[str, Any]:
    everywhere = dict(
        idle_timeout="24h",
        wait_for_workers=True,
        scheduler_vm_types=["m6i.xlarge"],
        shutdown_on_close=shutdown_on_close,
    )

    if scale == 1:
        return {
            "worker_vm_types": ["m6i.large"],
            "n_workers": 4,
            **everywhere,
        }
    if scale == 10:
        return {
            "worker_vm_types": ["m6i.large"],
            "n_workers": 8,
            **everywhere,
        }
    elif scale == 100:
        return {
            "worker_vm_types": ["m6i.large"],
            "n_workers": 16,
            **everywhere,
        }
    elif scale == 1000:
        return {
            "worker_vm_types": ["m6i.xlarge"],
            "n_workers": 32,
            "worker_disk_size": 128,
            **everywhere,
        }
    elif scale == 10000:
        return {
            "worker_vm_types": ["m6i.xlarge"],
            "n_workers": 32,
            "worker_disk_size": 100,
            **everywhere,
        }


def get_single_vm_spec(scale):
    if scale == 1:
        return {
            "vm_type": "m6i.2xlarge",
        }
    if scale == 10:
        return {
            "vm_type": "m6i.4xlarge",
        }
    elif scale == 100:
        return {
            "vm_type": "m6i.8xlarge",
        }
    elif scale == 1000:
        return {
            "vm_type": "m6i.32xlarge",
        }
    elif scale == 10000:
        return {
            "vm_type": "m6i.32xlarge",
            "disk_size": 1000,
        }
