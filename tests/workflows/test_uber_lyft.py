import pytest


def foo():
    return 1

@pytest.mark.client("uber_lyft")
def test_exploratory_analysis(client):
    """Run some exploratory aggs on the dataset"""
    def work():
        return foo()

    client.submit(work).result()
