import numpy as np

from .utils_test import scaled_array_shape


def test_scaled_array_shape():
    assert scaled_array_shape(1024, (2, "x"), dtype=bool) == (2, 512)
    assert scaled_array_shape(1024, (2, "x"), dtype=float) == (2, 64)
    assert scaled_array_shape(1024, (2, "x"), dtype=np.float64) == (2, 64)
    assert scaled_array_shape(1024, (2, "x")) == (2, 64)

    assert scaled_array_shape(16, ("x", "x"), dtype=bool) == (4, 4)
    assert scaled_array_shape(256, ("4x", "x"), dtype=bool) == (32, 8)
    assert scaled_array_shape(64, ("x", "x", "x"), dtype=float) == (2, 2, 2)

    assert scaled_array_shape("10kb", ("x", "1kb"), dtype=bool) == (10, 1000)
