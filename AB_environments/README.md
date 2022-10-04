# A/B testing

It's possible to run the Coiled Runtime benchmarks on A/B comparisons,
highlighting performance differences between different released versions
of dask, distributed, or any of the dependencies and/or between different
dask configs.

To run an A/B test:

### 1. Create a new branch

Branch from main, on the coiled repo itself. Preferably, call the branch
something meaningful, e.g. `AB/jobstealing`.
You *must* create the branch on the Coiled repo (`coiled/coiled-runtime`); CI
workflows will not work on a fork (`yourname/coiled-runtime`).

### 2. Create files in AB_environments/

Open the `AB_environments/` directory and rename/create files as needed.
Each A/B runtime is made of exactly two files:
- `AB_<name>.conda.yaml` (a conda environment file)
- `AB_<name>.dask.yaml` (a dask configuration file)

You may create as many A/B runtime configs as you want in a single `coiled-runtime`
branch.

The conda environment file can contain whatever you want, as long as it can run the
tests; e.g.

```yaml
channels:
  - conda-forge
dependencies:
    - python=3.9
    - coiled-runtime=0.1.0
    - pip:
      - dask==2022.9.0
      - distributed==2022.9.0
```
In this example it's using `coiled-runtime` as a base, but it doesn't have to. If you do
use `coiled-runtime` though, you must install any conflicting packages with pip; in the
example above, `coiled-runtime-0.1.0` pins `dask=2022.6.0` and `distributed=2022.6.0`,
so if you want to install a different version you need to use pip to circumvent the pin.

Instead of published packages, you could also used arbitrary git hashes of
arbitrary forks, e.g.

```yaml
    - pip:
      - dask==2022.9.0
      - git+https://github.com/yourname/distributed@1fd07f03cacee6fde81d13282568a727bce789b9
```
The second file in each pair is a dask config file. If you don't want to change the
config, you must create an empty file.

e.g.
```yaml
distributed:
  scheduler:
    work-stealing: False
```

### 3. Create baseline files
If you create *any* files in `AB_environments/`, you *must* create the baseline environment:

- `AB_baseline.conda.yaml`
- `AB_baseline.dask.yaml`

### 4. Tweak configuration file
Open `AB_environments/config.yaml` and set the `repeat` setting to a number higher than 0.
This enables the A/B tests.
Setting a low number of repeated runs is faster and cheaper, but will result in higher
variance.

`repeat` must remain set to 0 in the main branch, thus completely disabling
A/B tests, in order to avoid unnecessary runs.

In the same file, you can also set the `test_null_hypothesis` flag to true to
automatically create a verbatim copy of AB_baseline and then compare the two in the A/B
tests. Set it to false to save some money if you are already confident that the 'repeat'
setting is high enough.

Finally, the files offers a `categories` list. These are the subdirectories of `tests/`
which you wish to run.

### 5. (optional) Tweak tests
Nothing prevents you from changing the tests themselves.

For example, you may be interested in a single test, but you don't want to run its
whole category; all you need to do is open the test files and delete what you don't care
about.

### Complete example
You want to test the impact of disabling work stealing. You'll create at least 4 files:

- `AB_environments/AB_baseline.conda.yaml`:
```yaml
channels:
  - conda-forge
dependencies:
    - python=3.9
    - coiled-runtime=0.1.0
    - pip:
      - dask==2022.9.0
      - distributed==2022.9.0
```
- `AB_environments/AB_baseline.dask.yaml`: (empty file)
- `AB_environments/AB_no_steal.conda.yaml`: (same as baseline)
- `AB_environments/AB_no_steal.dask.yaml`:
```yaml
distributed:
  scheduler:
    work-stealing: False
```

- `AB_environments/config.yaml`:
```yaml
repeat: 5
test_null_hypothesis: true
categories:
  - runtime
  - benchmarks
  - stability
```

### 6. Run CI
- `git push`. Note: you should *not* open a Pull Request. 
- Open https://github.com/coiled/coiled-runtime/actions/workflows/ab_tests.yml and wait
  for the run to complete.
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
Your conda environment points to a fork of dask/dask or dask/distributed, but its owner
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
