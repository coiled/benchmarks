TPC-H Benchmarks
================

This document will help you run the TPC-H benchmarks in this directory.

Setup
-----

Clone this repository

```
git clone git@github.com:coiled/benchmarks
cd benchmarks
```

Follow the environment creation steps in the root directory. Namely the
following:

```
mamba env create -n tpch -f ci/environment.yml
conda activate tpch
mamba env update -f ci/environment-git-tip.yml
mamba env update -f ci/environment-test.yml
mamba install grpcio grpcio-status protobuf -y  # if you want Spark
```

Run Single Benchmark
--------------------

```
py.test --benchmark tests/tpch/test_dask.py
```

Configure
---------

By default we run Scale 100 (about 100 GB) on the cloud with Coiled.  You can
configure this by changing the values for `_local` and `_scale` in the
`conftest.py` file in this directory (they're at the top).

Local Data Generation
---------------------

If you want to run locally, you'll need to generate data.  Run the following
from the **root directory** of this repository.

```
python tests/tpch/generate-data.py --scale 10
```

Run Many Tests
--------------

When running on the cloud you can run many tests simultaneously.  We recommend
using pytest-xdist for this with the keywords:

-   `-n 4` run four parallel jobs
-   `--dist loadscope` split apart by module

```
py.test --benchmark -n 4 --dist loadscope tests/tpch
```

Generate Plots
--------------

Timing outputs are dropped into `benchmark.db` in the root of this repository.
You can generate charts analyzing results using either the notebook
`visualize.ipynb` in this directory (recommended) or the `generate-plot.py`
script in this directory.  These require `ibis` and `altair` (not installed
above).

These are both meant to be run from the root directory of this repository.

These pull out the most recent records for each query/library pairing.  If
you're changing scales and want to ensure clean results, you may want to nuke
your `benchmark.db` file between experiments (it's ok, it'll regenerate
automatically).
