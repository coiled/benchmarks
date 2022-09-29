from __future__ import annotations

import pytest
import xarray


@pytest.fixture(scope="module")
def cmip6():
    store = "s3://coiled-runtime-ci/CMIP6/CMIP/AS-RCEC/TaiESM1/1pctCO2/r1i1p1f1/Amon/zg/gn/v20200225/"
    ds = xarray.open_dataset(store, engine="zarr", chunks={})
    yield ds


def test_select_scalar(small_client, cmip6):
    ds = cmip6.isel({"lat": 20, "lon": 40, "plev": 5, "time": 1234}).compute()
    assert ds.zg.shape == ()
    assert ds.zg.size == 1
