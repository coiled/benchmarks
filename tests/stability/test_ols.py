import dask.array as da

from ..utils_test import wait


def test_ols(small_client, num_samples=int(1e8), num_coeffs=100, chunksize=int(1e6)):
    beta = da.random.normal(size=(num_coeffs,))
    X = da.random.normal(size=(num_samples, num_coeffs), chunks=(chunksize, -1))
    y = X @ beta + da.random.normal(size=(num_samples,), chunks=(chunksize,))
    beta_hat = da.linalg.solve(X.T @ X, X.T @ y)  # normal eq'n
    y_hat = X @ beta_hat
    wait(y_hat, small_client, 20 * 60)
