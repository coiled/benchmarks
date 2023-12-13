# Coiled Benchmarks

[![Tests](https://github.com/coiled/benchmarks/actions/workflows/tests.yml/badge.svg)](https://github.com/coiled/benchmarks/actions/workflows/tests.yml)
[![Linting](https://github.com/coiled/benchmarks/actions/workflows/lint.yml/badge.svg)](https://github.com/coiled/benchmarks/actions/workflows/lint.yml)
[![Benchmarks](https://shields.io/badge/-Benchmarks-blue)](https://benchmarks.coiled.io)

Set of Dask benchmarks run daily at scale in Coiled Clusters.

## Test Locally (for developers)

The `coiled benchmarks` test suite can be run locally with the following steps:

1. Ensure your local machine is authenticated to use the `dask-benchmarks` Coiled
   account and the Coiled Dask Engineering AWS S3 account.
2. Create a new Python environment with
   `mamba env create -n test-env -f ci/environment.yml`. You could alternatively use
   different packages, e.g. if you are testing feature branches of dask or distributed.
   This test suite is configured to run Coiled's `package_sync` feature, so your local
   environment will be copied to the cluster.
3. Activate the environment with `conda activate test-env`
4. Some tests require additional dependencies:
   - For snowflake: `mamba env update -f ci/environment-snowflake.yml`  
   - For non-dask TPCH: `mamba env update -f ci/environment-tpch-nondask.yml`

    Look at `ci/environment-*.yml` for more options.
5. Upgrade dask to the git tip with `mamba env update -f ci/environment-git-tip.yml`
6. Add test-specific packages with `mamba env update -f ci/environment-test.yml`
7. Run tests with `python -m pytest tests`.
   You may consider running instead individual tests or categories of tests; e.g.
   `python -m pytest tests -m shuffle_p2p`.
   Look at `setup.cfg` for all available test markers.

Additionally, tests are automatically run on pull requests to this repository.
See the section below on creating pull requests.

## Benchmarking

The `coiled-benchmarks` test suite contains a series of pytest fixtures which enable
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
# First, edit the `benchmark_schema.py` and commit the changes.

alembic revision --autogenerate -m "Description of migration"
git add alembic/versions/name_of_new_migration.py
git commit -m "Added a new migration"
```

Migrations are automatically applied in the pytest runs, so you needn't run them yourself.

### Deleting old test data

At times you might change a specific test that makes older benchmarking data irrelevant.
In that case, you can discard old benchmarking data for that test by applying a data migration
removing that data:

```bash
alembic revision -m "Declare bankruptcy for <test-name>"
# Edit the migration here to do what you want.
git add alembic/versions/name_of_new_migration.py
git commit -m "Bankruptcy migration for <test-name>"
```

An example of a migration that does this is [here](./alembic/versions/924e9b1430e1_spark_test_bankruptcy.py).

### Using the benchmark fixtures

We have a number of pytest fixtures defined which can be used to automatically track certain metrics in the benchmark database.
The most relevant ones are summarized here:

**`benchmark_time`**: Record wall clock time duration.

**`benchmark_memory`**: Record memory usage.

**`benchmark_task_durations`**: Record time spent computing, transferring data, spilling to disk, and deserializing data.

**`get_cluster_info`**: Record cluster id, name, etc.

**`benchmark_all`**: Record all available metrics.

For more information on all available fixtures and examples on how to use them, please refer to their documentation.

Writing a new benchmark fixture would generally look like:
1. Requesting the `test_run_benchmark` fixture, which yields an ORM object.
1. Doing whatever setup work you need.
1. `yield`ing to the test
1. Collecting whatever information you need after the test is done.
1. Setting the appropriate attributes on the ORM object.

The `benchmark_time` fixture provides a fairly simple example.

## Investigating performance regressions

It is not always obvious what the cause of a seeming performance regression is.
It could be due to a new version of a direct or transitive dependency,
and it could be due to a change in the Coiled platform.
But often it is due to a change in `dask` or `distributed`.
If you suspect that is the case, Coiled's `package_sync` feature combines well with the benchmarking infrastructure here and `git bisect`.

The following is an example workflow which could be used to identify a specific commit in `dask` which introduced a performance regression.
This workflow presumes you have two terminal windows open, one with the `coiled-runtime` test suite,
and one with a `dask` repository with which to drive bisecting.

#### Create your software environment

You should create a software environment which can run this test suite, but with an
editable install of `dask`.
You can do this in any of a number of ways, but one approach coule be
```bash
mamba env create -n test-env --file ci/environment.yml  # Create a test environment
conda activate test-env  # Activate your test environment
mamba env update --file ci/environment-test.yml
(cd <your-dask-dir> && pip install -e .)  # Do an editable install for dask
```

#### Start bisecting

Let's say the current `HEAD` of `dask` is known to be bad, and `$REF` is known to be
good. If you are looking at an upstream run where you have access to the static page,
you can check the dates reported for each run and do a `git log` with the corresponding
dates to get a list of commits to use in the bisecting process.

`git log --since='2022-08-15 14:15' --until='2022-08-18 14:15' --pretty=oneline`
In the terminal opened to your dask repository you can initialize a bisect workflow with

```bash
cd <your-dask-dir>
git bisect start
git bisect bad
git bisect good $REF
```

#### Test for regressions

Now that your editable install is bisecting, run a test or subset of tests which
demonstrate the regression in your `coiled-runtime` terminal.
Presume that `tests/benchmarks/test_parquet.py::test_write_wide_data` is such a test:

```bash
pytest tests/benchmarks/test_parquet.py::test_write_wide_data --benchmark
```

Once the test is done, it will have written a new entry to a local sqlite file `benchmark.db`.
You will want to check whether that entry displays the regression.
Exactly what that check will look like will depend on the test and the regression.
You might have a script that builds a chart from `benchmark.db` similar to `dashboard.py`,
or a script that performs some kind of statistical analysis.
But let's assume a simpler case where you can recognize it from the `average_memory`.
You can query that with

```bash
sqlite3 benchmark.db "select dask_version, average_memory from test_run where name = 'test_write_wide_data';"
```

If the last entry displays the regression, mark the commit in your dask terminal as `bad`:

```bash
git bisect bad
```

If the last entry doesn't display the regression, mark the commit in your dask terminal as `good`:

```bash
git bisect good
```

Proceed with this process until you have narrowed it down to a single commit.
Congratulations! You've identified the source of your regression.

## A/B testing

It's possible to run the Coiled Runtime benchmarks for A/B comparisons.
[Read full documentation](AB_environments/README.md).


## Contribute

This repository uses GitHub Actions secrets for managing authentication tokens used to
access resources like Coiled clusters, S3 buckets, etc. However, because GitHub Actions
[doesn't grant access to secrets for forked
repositories](https://docs.github.com/en/actions/security-guides/encrypted-secrets#using-encrypted-secrets-in-a-workflow),
**please submit pull requests directly from the `coiled/benchmarks` repository, not a
personal fork**.


## License

[BSD-3](LICENSE)
