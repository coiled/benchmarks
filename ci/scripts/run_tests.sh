set -o errexit
set -o nounset
set -o xtrace

# Ensure we run additional tests when testing the latest coiled-runtime
if [[ $COILED_RUNTIME_VERSION = 'latest' ]]
then
  export EXTRA_OPTIONS="--run-latest"
  export COILED_SOFTWARE_NAME=$COILED_SOFWARE_NAME
else
  export EXTRA_OPTIONS=" "
  unset COILED_SOFTWARE_NAME
fi

python -m pytest $EXTRA_OPTIONS tests/runtime "$@"
