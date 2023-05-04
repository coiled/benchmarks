import dask
import dask.array as da
import numpy as np
from dask_image import ndfilters, ndmeasure, ndmorph


def test_BBBC039(small_client):
    # Based off of https://github.com/GenevieveBuckley/dask-image-talk-2020
    images = da.from_zarr(
        "s3://coiled-datasets/BBBC039", storage_options={"anon": True}
    )
    smoothed = ndfilters.gaussian_filter(images, sigma=[0, 1, 1])
    thresh = ndfilters.threshold_local(smoothed, block_size=images.chunksize)
    threshold_images = smoothed > thresh
    # Since this image stack appears to be 3-dimensional,
    # we sandwich a 2d structuring element in between zeros
    # so that each 2d image slice has the binary closing applied independently
    structuring_element = np.array(
        [
            [[0, 0, 0], [0, 0, 0], [0, 0, 0]],
            [[0, 1, 0], [1, 1, 1], [0, 1, 0]],
            [[0, 0, 0], [0, 0, 0], [0, 0, 0]],
        ]
    )
    binary_images = ndmorph.binary_closing(
        threshold_images, structure=structuring_element
    )
    label_images, num_features = ndmeasure.label(binary_images)
    index = np.arange(num_features)
    # FIXME: Only selecting the first few images due to cluster idle timeout.
    # Maybe sending large graph? Need to investigate a bit.
    num_images = 10
    area = ndmeasure.area(images[:num_images], label_images[:num_images], index)
    mean_intensity = ndmeasure.mean(
        images[:num_images], label_images[:num_images], index
    )
    dask.compute(mean_intensity, area)
