# Coiled Distribution

Create conda recipe as follows:

1. Create and activate a conda environment and install conda-build
1. Clone the existing repository
2. From within the repository run: `conda build continuous_integration/recipe --output-folder dist/conda --no-anaconda-upload`
3. Install with: `conda install -c ./dist/conda/ coiled-distribution`
