def get_dataset_path(local, scale):
    remote_paths = {
        1: "s3://coiled-runtime-ci/tpc_h/scale-1/",
        10: "s3://coiled-runtime-ci/tpc_h/scale-10/",
        100: "s3://coiled-runtime-ci/tpc_h/scale-100/",
        1000: "s3://coiled-runtime-ci/tpc_h/scale-1000/",
        10000: "s3://coiled-runtime-ci/tpc_h/scale-10000/",
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
