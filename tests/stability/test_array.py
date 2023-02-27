import sys

import dask.array as da
import pytest

from ..utils_test import cluster_memory, scaled_array_shape, wait


@pytest.mark.stability
def test_rechunk_in_memory(small_client, configure_rechunking):
    x = da.random.random((50000, 50000))
    x.rechunk((50000, 20)).rechunk((20, 50000)).sum().compute()


@pytest.mark.skip(reason="this runs forever")
def test_rechunk_out_of_memory(small_client, configure_rechunking):
    x = da.random.random((100000, 100000))
    x.rechunk((50000, 20)).rechunk((20, 50000)).sum().compute()


@pytest.mark.stability
@pytest.mark.skipif(
    sys.platform.startswith("win"), reason="scaled_array_shape fails on windows"
)
def test_ols(small_client):
    chunksize = int(1e6)
    memory = cluster_memory(small_client)
    target_nbytes = memory * 0.50
    target_shape = scaled_array_shape(target_nbytes, ("x", 100))
    num_samples, num_coeffs = target_shape[0], target_shape[-1]
    beta = da.random.normal(size=(num_coeffs,))
    X = da.random.normal(size=(num_samples, num_coeffs), chunks=(chunksize, -1))
    y = X @ beta + da.random.normal(size=(num_samples,), chunks=(chunksize,))
    beta_hat = da.linalg.solve(X.T @ X, X.T @ y)  # normal eq'n
    y_hat = X @ beta_hat
    wait(y_hat, small_client, 20 * 60)
