# Coiled Distribution

Create conda recipe as follows:
1.  Create a conda environment & install conda-build 
2.  Clone the existing repo
3.  pushd conda-distribution
3.  run:  conda build . --output-folder dist/conda --no-anaconda-upload
4.  Install with:  conda install -c file://<CWD>/coiled-distribution/dist/conda coiled-distribution