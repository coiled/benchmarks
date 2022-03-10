# Coiled Distribution

Create conda recipe as follows:

1. Create and activate a conda environment and install conda-build
1. Clone the existing repository
2. From within the repository run: `conda build continuous_integration/recipe --output-folder dist/conda --no-anaconda-upload`
3. Install with: `conda install -c ./dist/conda/ coiled-distribution`

Releasing
---------

Make sure the version on the `meta.yaml` has been updated. Once that's on main proceed as follow.

.. code-block:: bash

   # Set next version number (matching version on `meta.yml`)
   export RELEASE=x.x.x

   # Create tags
   git commit --allow-empty -m "Release $RELEASE"
   git tag -a $RELEASE -m "Version $RELEASE"

   # Push
   git push upstream --tags
