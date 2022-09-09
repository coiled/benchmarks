set -o errexit
set -o nounset
set -o xtrace

BENCHMARK="${BENCHMARK:-false}"

# Ensure we run additional tests when testing the latest coiled-runtime
if [[ "$COILED_RUNTIME_VERSION" = 'latest' ]]
then
  EXTRA_OPTIONS="--run-latest"
  export COILED_SOFTWARE_NAME=$(cat software_name.txt)
  export TEST_UPSTREAM=$(cat test_upstream.txt)
elif [[ "$COILED_RUNTIME_VERSION" =~ AB_ ]]
then
  EXTRA_OPTIONS=""
  export COILED_SOFTWARE_NAME=$(cat software_name.txt)
  export TEST_UPSTREAM=$(cat test_upstream.txt)
else
  EXTRA_OPTIONS=""
  unset COILED_SOFTWARE_NAME
fi
if [[ $BENCHMARK = 'true' ]]
then
  EXTRA_OPTIONS="$EXTRA_OPTIONS --benchmark"
fi

python -m pytest -n 10 --dist loadscope $EXTRA_OPTIONS "$@"

