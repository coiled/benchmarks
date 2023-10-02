from __future__ import annotations

import math

import coiled
import dask
import dask.array as da
import dask.dataframe as dd
import distributed
import numpy as np
import pandas as pd
import pytest
from dask.datasets import timeseries
from dask.sizeof import sizeof
from dask.utils import format_bytes, parse_bytes


def scaled_array_shape(
    target_nbytes: float | str,
    shape: tuple[int | str, ...],
    *,
    dtype: np.dtype | type = np.dtype(float),
    max_error: float = 0.1,
) -> tuple[int, ...]:
    """
    Given a shape with free variables in it, generate the shape that results in the
    target array size.

    Example
    -------
    >>> scaled_array_shape(1024, (2, "x"), dtype=bool)
    (2, 512)
    >>> scaled_array_shape(2048, (2, "x"), dtype=bool)
    (2, 1024)
    >>> scaled_array_shape(16, ("x", "x"), dtype=bool)
    (4, 4)
    >>> scaled_array_shape(256, ("4x", "x"), dtype=bool)
    (32, 8)
    >>> scaled_array_shape("10kb", ("x", "1kb"), dtype=bool)
    (10, 1000)
    """
    target_nbytes = parse_bytes(target_nbytes)

    dtype = np.dtype(dtype)
    # Given a shape like:
    # (10, "2x", 3, "x", 50)
    # We're solving for x in:
    # `10 * 2x * 3 * x * 50 * dtype.itemsize == target_nbytes`
    # aka:
    # `3000x^2 * dtype.itemsize == target_nbytes`
    resolved_shape: list[int | None] = []
    x_locs_coeffs: list[tuple[int, float]] = []
    total_coeff = 1
    for i, s in enumerate(shape):
        if isinstance(s, str):
            if s[-1] == "x":
                coeff = 1 if len(s) == 1 else float(s[:-1])
                assert coeff > 0, coeff
                x_locs_coeffs.append((i, coeff))
                total_coeff *= coeff
                resolved_shape.append(None)
                continue
            else:
                s = parse_bytes(s) // dtype.itemsize

        assert s > 0, s
        total_coeff *= s
        resolved_shape.append(s)

    assert x_locs_coeffs, f"Expected at least 1 `x` value in shape {shape}"
    total_coeff *= dtype.itemsize
    x = (target_nbytes / total_coeff) ** (1 / len(x_locs_coeffs))

    # Replace `x` values back into shape
    for i, coeff in x_locs_coeffs:
        assert resolved_shape[i] is None
        resolved_shape[i] = round(coeff * x)

    final = tuple(s for s in resolved_shape if s is not None)
    assert len(final) == len(resolved_shape), resolved_shape

    actual_nbytes = math.prod(final) * dtype.itemsize
    error = (actual_nbytes - target_nbytes) / actual_nbytes
    assert abs(error) < max_error, (
        error,
        actual_nbytes,
        target_nbytes,
        final,
        dtype.itemsize,
    )
    return final


def scaled_array_shape_quadratic(
    target_nbytes: float | str,
    baseline_nbytes: float | str,
    shape: tuple[int | str, ...],
    *,
    dtype: np.dtype | type = np.dtype(float),
    max_error: float = 0.1,
) -> tuple[int, ...]:
    """Variant of `scaled_array_shape` for algorithms that scale quadratically with the
    amount of data. This function automatically reduces `target_nbytes` when larger than
    baseline_nbytes, and increases it when smaller, in order to keep a constant-ish
    runtime.
    """
    target_nbytes = parse_bytes(target_nbytes)
    baseline_nbytes = parse_bytes(baseline_nbytes)
    scaled_nbytes = int(baseline_nbytes * math.sqrt(target_nbytes / baseline_nbytes))
    return scaled_array_shape(scaled_nbytes, shape, dtype=dtype, max_error=max_error)


def downscale_dataframe(ddf: dd.DataFrame, target_memory: float | str) -> dd.DataFrame:
    """Dynamically reduce the size of a dask DataFrame depending on how much RAM is
    available on the cluster.

    Parameters
    ----------
    ddf: dask.dataframe.DataFrame
        Input dataframe, typically just loaded from s3
    target_memory: float | str
        Minimum size of the cluster that would not require downscaling

    If the cluster mounts less than the target memory, pick a random subset of the
    input partitions and forget about the rest. If this call is immediately after
    read_parquet() / read_csv(), the remaining partitions will never leave the disk.

    This is similar to writing:
    >>> available_memory = cluster_memory(distributed.get_client())
    >>> scale = min(1.0, available_memory / target_memory)
    >>> ddf, _ = ddf.random_split([scale, 1 - scale])

    but instead of reading everything from disk, it only reads selected partitions.
    This works well as long as
    1. all partitions are roughly the same size, and
    2. data is uniformely spread across partitions.

    Note that the latter would likely be a very big and unpalatable assumption in a data
    science context; here it's OK because we're focusing on performance testing.
    """
    target_memory = parse_bytes(target_memory)

    client = distributed.get_client()
    memory = cluster_memory(client)
    npartitions = ddf.npartitions * memory // target_memory
    if npartitions >= ddf.npartitions:
        return ddf

    # Fixed random seed is important to avoid unpredictable behaviour when the input
    # partitions are not the same size: if you run twice using the same cluster size,
    # you'll get exactly the same partitions.
    rng = np.random.RandomState(0)
    idx = rng.choice(np.arange(ddf.npartitions), npartitions, replace=False)
    return ddf.partitions[idx]


def print_size_info(memory: int, target_nbytes: float, *arrs: da.Array) -> None:
    print(
        f"Cluster memory: {format_bytes(memory)}, "
        f"target data size: {format_bytes(target_nbytes)}"
    )
    for i, arr in enumerate(arrs, 1):
        print(
            f"Input {i}: {format_bytes(arr.nbytes)} - {arr.npartitions} "
            f"{format_bytes(arr.blocks[(0,) * arr.ndim].nbytes)} chunks"
        )


def wait(thing, client, timeout):
    "Like `distributed.wait(thing.persist())`, but if any tasks fail, raises its error."

    p = thing.persist() if hasattr(thing, "persist") else thing
    try:
        distributed.wait(p, timeout=timeout)
        for f in client.futures_of(p):
            if f.status in ("error", "cancelled"):
                raise f.exception()
    finally:
        client.cancel(p)


def cluster_memory(client: distributed.Client) -> int:
    """Total memory available on the cluster, in bytes"""
    return int(
        sum(w["memory_limit"] for w in client.scheduler_info()["workers"].values())
    )


def timeseries_of_size(
    target_nbytes: int | str,
    *,
    start="2000-01-01",
    freq="1s",
    partition_freq="1d",
    dtypes={"name": str, "id": int, "x": float, "y": float},
    seed=None,
    **kwargs,
) -> dd.DataFrame:
    """
    Generate a `dask.demo.timeseries` of a target total size.

    Same arguments as `dask.demo.timeseries`, but instead of specifying an ``end`` date,
    you specify ``target_nbytes``. The number of partitions is set as necessary to reach
    approximately that total dataset size. Note that you control the partition size via
    ``freq``, ``partition_freq``, and ``dtypes``.

    Examples
    --------
    >>> timeseries_of_size(
    ...     "1mb", freq="1s", partition_freq="100s", dtypes={"x": float}
    ... ).npartitions
    278
    >>> timeseries_of_size(
    ...     "1mb", freq="1s", partition_freq="100s", dtypes={i: float for i in range(10)}
    ... ).npartitions
    93

    Notes
    -----
    The ``target_nbytes`` refers to the amount of RAM the dask DataFrame would use up
    across all workers, as many pandas partitions.

    This is typically larger than ``df.compute()`` would be as a single pandas
    DataFrame. Especially with many partions, there can be significant overhead to
    storing all the individual pandas objects.

    Additionally, ``target_nbytes`` certainly does not correspond to the size
    the dataset would take up on disk (as parquet, csv, etc.).
    """
    if isinstance(target_nbytes, str):
        target_nbytes = parse_bytes(target_nbytes)

    start_dt = pd.to_datetime(start)
    partition_freq_dt = pd.to_timedelta(partition_freq)
    example_part = timeseries(
        start=start,
        end=start_dt + partition_freq_dt,
        freq=freq,
        partition_freq=partition_freq,
        dtypes=dtypes,
        seed=seed,
        **kwargs,
    )
    p = example_part.compute(scheduler="threads")
    partition_size = sizeof(p)
    npartitions = round(target_nbytes / partition_size)
    assert npartitions > 0, (
        f"Partition size of {format_bytes(partition_size)} > "
        f"target size {format_bytes(target_nbytes)}"
    )

    ts = timeseries(
        start=start,
        end=start_dt + partition_freq_dt * npartitions,
        freq=freq,
        partition_freq=partition_freq,
        dtypes=dtypes,
        seed=seed,
        **kwargs,
    )
    assert ts.npartitions == npartitions
    return ts


def arr_to_devnull(arr: da.Array) -> dask.delayed:
    "Simulate storing an array to zarr, without writing anything (just drops every block once it's computed)"

    # NOTE: this class must be defined inside the function so it's cloudpickled as code,
    # otherwise `tests/utils_test` would have to be installed on the cluster.
    class _DevNull:
        def __setitem__(self, k, v):
            pass

    # TODO `da.store` should use blockwise to be much more efficient https://github.com/dask/dask/issues/9381
    return da.store(arr, _DevNull(), lock=False, compute=False)


def ec2_instance_cpus(name: str) -> int:
    kind, _, size = name.partition(".")

    if size == "large":
        return 2
    if size == "xlarge":
        return 4
    if size.endswith("xlarge"):
        return int(size[len("xlarge") :]) * 4

    # nano, micro, small, and medium instances are probably uninteresting for dask.
    # metal is interesting, but every architecture has its own number of cores and it
    # would be too much of an effort to hardcode them all here.

    raise ValueError(f"Unknown instance type: {name}")


def run_up_to_nthreads(
    cluster_name: str,
    nthreads_max: int,
    reason: str | None = None,
    as_decorator: bool = True,
):
    from .conftest import load_cluster_kwargs

    cluster_kwargs = load_cluster_kwargs()[cluster_name]
    nworkers = cluster_kwargs["n_workers"]
    # This does not consider that there may be a fallback to smaller/larger workers.
    # It doesn't really make sense to have fallbacks in case of performance benchmarks
    # anyway.
    instance_type = cluster_kwargs["worker_vm_types"][0]
    nthreads = nworkers * ec2_instance_cpus(instance_type)

    reason = reason or "cluster too large"
    if as_decorator:
        return pytest.mark.skipif(nthreads > nthreads_max, reason=reason)
    elif nthreads > nthreads_max:
        raise pytest.skip(reason)


def get_cluster(*args, **kwargs):
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            return coiled.Cluster(*args, **kwargs)
        except coiled.errors.ClusterCreationError as err:
            if "Cluster was waiting for" in str(err):
                print(
                    f"Cluster creation failed at attempt {attempt} / {max_attempts}. Retrying..."
                )
                continue

    raise RuntimeError(
        f"Cluster failed to come up with all workers after {max_attempts} attempts."
    )
