import dask.array as da
import pytest


def test_rechunk_in_memory(small_client):
    x = da.random.random((50000, 50000))
    x.rechunk((50000, 20)).rechunk((20, 50000)).sum().compute()

    def raiser():
        raise RuntimeError("oops!")

    small_client.run(raiser)


@pytest.mark.skip(reason="this runs forever")
def test_rechunk_out_of_memory(small_client):
    x = da.random.random((100000, 100000))
    x.rechunk((50000, 20)).rechunk((20, 50000)).sum().compute()
