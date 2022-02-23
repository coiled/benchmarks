# Coiled Distribution

Create conda recipe as follows:
1. Clone the existing repo
2. run:  conda build <CWD>/coiled-distribution/conda-recipe --output-folder <CWD>/coiled-distribution/dist/conda --no-anaconda-upload

3. install with:  conda install -c file:/<CWD>/coiled-distribution/dist/conda coiled-distribution