# Coiled Runtime

[![Tests](https://github.com/coiled/coiled-runtime/actions/workflows/tests.yml/badge.svg)](https://github.com/coiled/coiled-runtime/actions/workflows/tests.yml) [![Linting](https://github.com/coiled/coiled-runtime/actions/workflows/lint.yml/badge.svg)](https://github.com/coiled/coiled-runtime/actions/workflows/lint.yml)

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

To issue a new `coiled-runtime` release:

1. Locally update the `coiled-runtime` version and package pinnings specified in `recipe/meta.yaml`.
    - When updating package version pinnings (in particular `dask` and `distributed`)
      confirm there are no reported large scale stability issues (e.g. deadlocks) or
      performance regressions on the `dask` / `distributed` issue trackers or offline
      reports.
2. Open a pull request to the `coiled-runtime` repository titled "Release X.Y.Z" with these changes
   (where `X.Y.Z` is replaced with the actual version for the release).
3. After all CI builds have passed the release pull request can be merged.
4. Add a new git tag for the release by following the steps below on your local machine:

```bash
# Pull in changes from the Release X.Y.Z PR
git checkout main
git pull origin main

# Set release version number
export RELEASE=X.Y.Z
# Create and push release tag
git tag -a $RELEASE -m "Version $RELEASE"
git push origin main --tags
```

5. Update the `coiled-runtime` package on conda-forge by opening a pull request to the
   [`coiled-runtime` conda-forge feedstock](https://github.com/conda-forge/coiled-runtime-feedstock)
   which updates the `coiled-runtime` version and package version pinnings.
    - Note that pull requests to conda-forge feedstocks must come from a fork.
    - Reset the build number back to `0` if it isn't already.
    - For more information on updating conda-forge packages, see the
      [conda-forge docs](https://conda-forge.org/docs/maintainer/updating_pkgs.html).

## License

[BSD-3](LICENSE)
