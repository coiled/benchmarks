set -o errexit
set -o nounset
set -o xtrace

BENCHMARK="${BENCHMARK:-false}"

# Ensure we run additional tests when testing the latest coiled-runtime
if [[ $COILED_RUNTIME_VERSION = 'latest' || $COILED_RUNTIME_VERSION = 'upstream' ]]
then
  EXTRA_OPTIONS="--run-latest"
else
  EXTRA_OPTIONS=""
fi
if [[ $BENCHMARK = 'true' ]]
then
  EXTRA_OPTIONS="$EXTRA_OPTIONS --benchmark"
fi

python -m pytest -n 8 --dist loadscope $EXTRA_OPTIONS $@
