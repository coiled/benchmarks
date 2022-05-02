set -o errexit
set -o nounset
set -o xtrace

# Ensure we run additional tests when testing the latest coiled-runtime
if [[ $COILED_RUNTIME_VERSION = 'latest' ]]
then
  export EXTRA_OPTIONS="--run-latest"
  # Construct latest software environment name
  export PYTHON_VERSION_FORMATTED=$(python -c "import sys; print(f'{sys.version_info[0]}{sys.version_info[1]}')")
  export COILED_SOFWARE_NAME=$COILED_SOFWARE_NAME-py$PYTHON_VERSION_FORMATTED
else
  export EXTRA_OPTIONS=" "
  unset COILED_SOFTWARE_NAME
fi

python -m pytest $EXTRA_OPTIONS "$@"
