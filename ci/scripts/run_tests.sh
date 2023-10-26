set -o errexit
set -o nounset
set -o xtrace

BENCHMARK="${BENCHMARK:-false}"

EXTRA_OPTIONS=""
if [[ $BENCHMARK = 'true' ]]
then
  EXTRA_OPTIONS="$EXTRA_OPTIONS --benchmark"
fi

python -m pytest $EXTRA_OPTIONS $@ tests/{benchmarks,stability,workflows,tpch/test_dask.py}
