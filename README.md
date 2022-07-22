# Coiled Runtime

[![Tests](https://github.com/coiled/coiled-runtime/actions/workflows/tests.yml/badge.svg)](https://github.com/coiled/coiled-runtime/actions/workflows/tests.yml)
[![Linting](https://github.com/coiled/coiled-runtime/actions/workflows/lint.yml/badge.svg)](https://github.com/coiled/coiled-runtime/actions/workflows/lint.yml)
[![Benchmarks](https://shields.io/badge/-Benchmarks-blue)](https://coiled.github.io/coiled-runtime/index.html)

The Coiled Runtime is a conda metapackage which makes it easy to get started with Dask.

## Install

`coiled-runtime` can be installed with:

```bash
conda install -c conda-forge coiled-runtime
```

**Nightly builds**
  
`coiled-runtime` has nightly conda packages for testing purposes.
You can install a nightly version of `coiled-runtime` with:

```bash
conda install -c coiled/label/dev -c dask/label/dev coiled-runtime 
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

## Test

The `coiled-runtime` test suite can be run locally with the following steps:

1. Ensure your local machine is authenticated to use the `dask-engineering` Coiled account and
   the Coiled Dask Engineering AWS S3 account.
2. Create a Python environment and install development dependencies as
   specified in `ci/environment.yml`.
3. (Optional) If testing against an unreleased version of `coiled-runtime`,
   create a Coiled software with the unreleased `coiled-runtime` installed
   and set a local `COILED_SOFTWARE_NAME` environment variable to the name
   of the software environment (e.g. `export COILED_SOFTWARE_NAME="account/software-name"`)
4. Run tests with `python -m pytest tests`

Additionally, tests are automatically run on pull requests to this repository.
See the section below on creating pull requests.

## Benchmarking

The `coiled-runtime` test suite contains a series of pytest fixtures which enable
benchmarking metrics to be collected and stored for historical and regression analysis.
By default, these metrics are not collected and stored, but they can be enabled
by including the `--benchmark` flag in your pytest invocation.

From a high level, here is how the benchmarking works:

* Data from individual test runs are collected and stored in a local sqlite database.
  The schema for this database is stored in `benchmark_schema.py`
* The local sqlite databases are appended to a global historical record, stored in S3.
* The historical data can be analyzed using any of a number of tools.
  `dashboard.py` creates a set of static HTML documents showing historical data for the tests.

### Running the benchmarks locally

You can collect benchmarking data by running pytest with the `--benchmark` flag.
This will create a local `benchmark.db` sqlite file in the root of the repository.
If you run a test suite multiple times with benchmarking,
the data will be appended to the database.

You can compare with historical data by downloading the global database from S3 first:

```bash
aws s3 cp s3://coiled-runtime-ci/benchmarks/benchmark.db ./benchmark.db
pytest --benchmark
```

### Changing the benchmark schema

You can add, remove, or modify columns by editing the SQLAlchemy schema in `benchmark_schema.py`.
However, if you have a database of historical data, then the schemas of the new and old data will not match.
In order to account for this, you must provide a migration for the data and commit it to the repository.
We use `alembic` to manage SQLAlchemy migrations.
In the simple case of simply adding or removing a column to the schema, you can do the following:

```bash
# First, edit the `benchmark_schema.py`

alembic revision --autogenerate -m "Description of migration"
git add alembic/versions/name_of_new_migration.py
git commit -m "Added a new migration"
```

Migrations are automatically applied in the pytest runs, so you needn't run them yourself.

### Using the benchmark fixtures

We have a number of pytest fixtures defined which can be used to automatically track certain metrics in the benchmark database.
They are summarized here:

**`benchmark_db_engine`**: The SQLAlchemy engine for the benchmark sqlite database. You can control the database name with the environment variable `DB_NAME`, which defaults to `benchmark.db`. Most tests shouldn't need to include this fixture directly.

**`benchmark_db_session`**: The SQLAlchemy session for a given test. Most tests shouldn't need to include this fixture directly.

**`test_run_benchmark`**: The SQLAlchemy ORM object for a given test. By including this fixutre in a test (or another fixture that includes it) you trigger the test being written to the benchmark database. This fixture includes data common to all tests, including python version, test name, and the test outcome.

**`benchmark_time`**: Include this fixture to measure the wall clock time.

**`sample_memory`**: This fixture yields a context manager which takes a distributed `Client` object, and records peak and average memory usage for the cluster within the context:
```python
def test_something(sample_memory):
    with Client() as client:
        with sample_memory(client):
            client.submit(expensive_function)
```

Writing a new benchmark fixture would generally look like:
1. Requesting the `test_run_benchmark` fixture, which yields an ORM object.
1. Doing whatever setup work you need.
1. `yield`ing to the test
1. Collecting whatever information you need after the test is done.
1. Setting the appropriate attributes on the ORM object.

The `benchmark_time` fixture provides a fairly simple example.

## Contribute

This repository uses GitHub Actions secrets for managing authentication tokens used
to access resources like Coiled clusters, S3 buckets, etc. However, because GitHub Actions [doesn't
grant access to secrets for forked repositories](https://docs.github.com/en/actions/security-guides/encrypted-secrets#using-encrypted-secrets-in-a-workflow),
**please submit pull requests directly from the `coiled/coiled-runtime` repository,
not a personal fork**.

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
