import coiled


def test_reconnect(small_cluster):
    """How quickly can we reconnect to an existing cluster?"""
    with coiled.Cluster(name=small_cluster.name):
        pass
