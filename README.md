# Coiled Runtime

[![Tests](https://github.com/coiled/coiled-runtime/actions/workflows/tests.yml/badge.svg)](https://github.com/coiled/coiled-runtime/actions/workflows/tests.yml) [![Linting](https://github.com/coiled/coiled-runtime/actions/workflows/lint.yaml/badge.svg)](https://github.com/coiled/coiled-runtime/actions/workflows/lint.yaml)

The Coiled Runtime is a conda metapackage which makes it easy to get started with Dask.

## Install

`coiled-runtime` can be installed with:

```bash
conda install -c coiled -c conda-forge coiled-runtime
```

## Build

To build and install `coiled-runtime` locally, use the following steps:

```bash
# Have a local copy of the `coiled-runtime` repository
git clone https://github.com/coiled/coiled-runtime
cd coiled-runtime

# Make sure conda-build is installed
conda install -c conda-forge conda-build

# Build the metapackage
conda build recipe -c conda-forge --output-folder dist/conda --no-anaconda-upload

# Install the built `coiled-runtime` metapackage
conda install -c conda-forge -c ./dist/conda/ coiled-runtime
```

## Release

To issue a new `coiled-runtime` release, update the `coiled-runtime` version specified in `meta.yaml`, then:

```bash
# Set next version number (matching version on `meta.yml`)
export RELEASE=x.x.x

# Create tags
git commit -m "Release $RELEASE"
git tag -a $RELEASE -m "Version $RELEASE"

# Push
git push upstream main --tags
```

## License

[BSD-3](LICENSE)
