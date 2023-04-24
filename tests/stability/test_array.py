import sys

import dask.array as da
import pytest

from ..utils_test import cluster_memory, scaled_array_shape, wait

try:
    import scipy  # noqa: F401

    has_scipy = True
except ImportError:
    has_scipy = False


@pytest.mark.stability
@pytest.mark.skipif(
    sys.platform.startswith("win"), reason="scaled_array_shape fails on windows"
)
@pytest.mark.skipif(not has_scipy, reason="requires scipy")
def test_ols(small_client):
    chunksize = int(1e6)
    memory = cluster_memory(small_client)
    target_nbytes = memory * 0.50
    target_shape = scaled_array_shape(target_nbytes, ("x", 100))
    num_samples, num_coeffs = target_shape[0], target_shape[-1]
    rng = da.random.default_rng()
    beta = rng.normal(size=(num_coeffs,))
    X = rng.normal(size=(num_samples, num_coeffs), chunks=(chunksize, -1))
    y = X @ beta + rng.normal(size=(num_samples,), chunks=(chunksize,))
    beta_hat = da.linalg.solve(X.T @ X, X.T @ y)  # normal eq'n
    y_hat = X @ beta_hat
    wait(y_hat, small_client, 20 * 60)
