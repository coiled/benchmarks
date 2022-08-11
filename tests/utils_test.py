import numpy as np
from dask.utils import parse_bytes
import distributed


def scaled_array_shape(
    target_nbytes: int | str,
    shape: tuple[int | str, ...],
    *,
    dtype: np.dtype | type = np.dtype(float),
    max_error: float = 0.1,
) -> tuple[int, ...]:
    """
    Given a shape with free variables in it, generate the shape that results in the target array size.

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
    if isinstance(target_nbytes, str):
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

    actual_nbytes = np.prod(final) * dtype.itemsize
    error = (actual_nbytes - target_nbytes) / actual_nbytes
    assert abs(error) < max_error, (error, actual_nbytes, target_nbytes, final)
    return final


def wait(thing, client, timeout):
    "Like `distributed.wait(thing.persist())`, but if any tasks fail, raises its error."
    p = thing.persist()
    try:
        distributed.wait(p, timeout=timeout)
        for f in client.futures_of(p):
            if f.status in ("error", "cancelled"):
                raise f.exception()
    finally:
        client.cancel(p)


def cluster_memory(client: distributed.Client) -> int:
    "Total memory available on the cluster, in bytes"
    return int(
        sum(w["memory_limit"] for w in client.scheduler_info()["workers"].values())
    )
