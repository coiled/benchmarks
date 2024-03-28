# A/B testing

It's possible to run the Coiled Runtime benchmarks on A/B comparisons,
highlighting performance differences between different released versions
of dask, distributed, or any of the dependencies and/or between different
dask configs.

To run an A/B test:

### 1. Create a new branch

Branch from main, on the coiled repo itself. Preferably, call the branch
something meaningful, e.g. `AB/jobstealing`.
You *must* create the branch on the Coiled repo (`coiled/benchmarks`); CI
workflows will not work on a fork (`yourname/benchmarks`).

### 2. Create files in AB_environments/

Open the `AB_environments/` directory and rename/create files as needed.
Each A/B runtime is made of exactly four files:
- `AB_<name>.cluster.yaml` (coiled.Cluster kwargs)
- `AB_<name>.conda.yaml` (a conda environment file, specifying python version and 
  non-pip packages)
- `AB_<name>.dask.yaml` (a dask configuration file)
- `AB_<name>.requirements.in` (a pip-compile requirements file)

You may create as many A/B runtime configs as you want in a single `coiled/benchmarks`
branch.
You can use the utility `make_envs.py <name>, [name], ...` to automate file creation.

`AB_<name>.requirements.in` can contain whatever you want, as long as it can run the
tests; e.g.

```
-r ../ci/requirements-1general-deps.in
-r ../ci/requirements-1test.in
dask==2024.3.1
distributed==2024.3.1
dask-expr==1.0.0
```

Instead of published packages, you could also use arbitrary git hashes of arbitrary
forks, e.g.
```
-r ../ci/requirements-1general-deps.in
-r ../ci/requirements-1test.in
git+https://github.com/dask/dask@b85bf5be72b02342222c8a0452596539fce19bce
git+https://github.com/yourname/distributed@803c624fcef99e3b6f3f1c5bce61a2fb4c9a1717
git+https://github.com/otherguy/dask-expr@0aab0e0f1e4cd91b15df19512493ffe05bffb73a
```

You may also ignore the default environment and go for a barebones environment. The bare
minimum you need to install is ``dask``, ``distributed``, ``coiled`` and ``s3fs``.
This will however skip some tests, e.g. zarr and ML-related ones, and it will also
expose you to less controlled behaviour e.g. dependent on which versions of numpy and
pandas are pulled in:
```
    -r ../ci/requirements-1test.in
    dask ==2023.4.1
    distributed ==2023.4.1
    coiled
    s3fs
```

**Note:** CI invokes `pip-compile` internally. Any important pins must be done in the
`.in` file. You can retrieve the output of `mamba list --export` from the CI log if you
want to reproduce an exact environment locally.

`AB_<name>.dask.yaml` is a dask config file. If you don't want to change the config,
you must create an empty file.

e.g.
```yaml
distributed:
  scheduler:
    work-stealing: False
```

`AB_<name>.cluster.yaml` defines creation options to the dask cluster. It must be
formatted as follows:
```yaml
default:
  <kwarg>: <value>
  ...

<cluster name>:
  <kwarg>: <value>
  ...
 ```
`<cluster name>` must be:
- `small_cluster`, for all tests decorated with `small_cluster`
- `parquet_cluster`, for all tests in `test_parquet.py`
- others: please refer to `cluster_kwargs.yaml`

The `default` section applies to all `<cluster name>` sections, unless explicitly
overridden.

Anything that's omitted defaults to the contents of `../cluster_kwargs.yaml`. 
Leave this file blank if you're happy with the defaults.

For example:
```yaml
small_cluster:
  n_workers: 10
  worker_vm_types: [m6i.large]  # 2CPU, 8GiB
```


### 3. Create baseline files
If you create *any* files in `AB_environments/`, you *must* create the baseline
environment:

- `AB_baseline.cluster.yaml`
- `AB_baseline.conda.yaml`
- `AB_baseline.dask.yaml`
- `AB_baseline.requirements.in`

### 4. Tweak configuration file
Open `AB_environments/config.yaml` and set the `repeat` setting to a number higher than 0.
This enables the A/B tests.
Setting a low number of repeated runs is faster and cheaper, but will result in higher
variance. Setting it to 5 is a good value to get statistically significant results.

`repeat` must remain set to 0 in the main branch, thus completely disabling
A/B tests, in order to avoid unnecessary runs.

In the same file, you can also set the `test_null_hypothesis` flag to true to
automatically create a verbatim copy of AB_baseline and then compare the two in the A/B
tests. Set it to false to save some money if you are already confident that the 'repeat'
setting is high enough.

The file offers a `targets` list. These can be test directories, individual test files,
or individual tests that you wish to run.

The file offers a `markers` string expression, to be passed to the `-m` pytest parameter
if present. See setup.cfg for the available ones.

`h2o_datasets` is a list of datasets to run through in
`tests/benchmarks/test_h2o.py`. Refer to the file for the possible choices.

Finally, the `max_parallel` setting lets you tweak maximum test parallelism, both in
github actions and in pytest-xdist. Reducing parallelism is useful when testing on very
large clusters (e.g. to avoid having 20 clusters with 1000 workers each at the same
time).


### 5. (optional) Tweak tests
Nothing prevents you from changing the tests themselves; for example, you may be
interested in some specific test, but on double the regular size, half the chunk size,
etc.


### Complete example
You want to test the impact of disabling work stealing on the latest version of dask.
You'll create at least 4 files:

- `AB_environments/AB_baseline.conda.yaml`:
```yaml
channels:
  - conda-forge
dependencies:
    - python =3.9
    - pip
    - pip-tools
```
  
- `AB_environments/AB_baseline.requirements.in`:

```
    -r ../ci/requirements-1general-deps.in
    -r ../ci/requirements-1test.in
    coiled
    dask
    distributed
    s3fs
```

- `AB_environments/AB_baseline.dask.yaml`: (empty file)
- `AB_environments/AB_baseline.cluster.yaml`: (empty file)
- `AB_environments/AB_no_steal.conda.yaml`: (same as baseline)
- `AB_environments/AB_no_steal.requirements.in`: (same as baseline)
- `AB_environments/AB_no_steal.dask.yaml`:
```yaml
distributed:
  scheduler:
    work-stealing: False
```
- `AB_environments/AB_no_steal.cluster.yaml`: (empty file)
- `AB_environments/config.yaml`:
```yaml
repeat: 5
test_null_hypothesis: true
targets:
  - tests/benchmarks
h2o_datasets:
  - 5 GB (parquet+pyarrow)
max_parallel:
  ci_jobs: 5
  pytest_workers_per_job: 4
```

### 6. Run CI
- `git push`. Note: you should *not* open a Pull Request. 
- Open [the GitHub Actions tab] 
  (https://github.com/coiled/benchmarks/actions/workflows/ab_tests.yml)
  and wait for the run to complete.
- Open the run from the link above. In the Summary tab, scroll down and download the
  `static-dashboard` artifact. 
  Note: artifacts will appear only after the run is complete.
- Decompress `static-dashboard.zip` and open `index.html` in your browser.


### 7. Clean up
Remember to delete the branch once you're done.


### Troubleshooting

#### Problem:
Environment build fails with a message such as:
> coiled 0.2.27 requires distributed>=2.23.0, but you have distributed 2.8.0+1709.ge0932ec2 which is incompatible.

#### Solution:
Your pip environment points to a fork of dask/dask or dask/distributed, but its owner
did not synchronize the tags. To fix, the owner of the fork must run:
```bash
$ git remote -v
origin  https://github.com/yourname/distributed.git (fetch)
origin  https://github.com/yourname/distributed.git (push)
upstream        https://github.com/dask/distributed.git (fetch)
upstream        https://github.com/dask/distributed.git (push)
$ git fetch upstream --tags  # Or whatever name dask was added as above
$ git push origin --tags     # Or whatever name the fork was added as above
```

#### Problem:
The conda environment fails to build, citing incompatibilities with openssl

#### Solution:
Double check that you didn't accidentally type `- python ==3.9`, which means 3.9.0,
instead of `- python =3.9`, which means the latest available patch version of 3.9.

#### Problem:
You get very obscure failures in the workflows, which you can't seem to replicate

#### Solution:
Double check that you don't have the same packages listed as conda package and under the
special `- pip:` tag. Installing a package with conda and then upgrading it with pip
*typically* works, but it's been observed not to (e.g. xgboost).

Specifically, `dask` and `distributed` *can* be installed with conda and then upgraded
with pip, *but* they must be *both* upgraded. Note that specifying the same version with
pip of a package won't upgrade it.
