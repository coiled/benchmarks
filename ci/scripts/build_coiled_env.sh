# If testing the latest `coiled-runtime`, we need to build a Coiled software environment
# that can be used when running tests
# Coiled software environment names can't contain "." or uppercase characters
set -o errexit
set -o nounset
set -o xtrace

export PYTHON_VERSION_FORMATTED=$(echo "$PYTHON_VERSION" | sed 's/\.//g' )
export OS_FORMATTED=$(python -c "import os; print(os.environ['RUNNER_OS'].lower())")
export REF_NAME_FORMATTED=$(echo "$GITHUB_REF_NAME" | sed 's/\./-/g' )
export COILED_SOFTWARE_NAME_HEAD=dask-engineering/coiled-runtime-$GITHUB_EVENT_NAME
export COILED_SOFTWARE_NAME_TAIL=$GITHUB_RUN_ID-$OS_FORMATTED-py$PYTHON_VERSION_FORMATTED

if [[ $GITHUB_EVENT_NAME = 'pull_request' ]]
then
  export COILED_SOFTWARE_NAME=$COILED_SOFTWARE_NAME_HEAD-$GITHUB_EVENT_NUMBER-$COILED_SOFTWARE_NAME_TAIL
else
  export COILED_SOFTWARE_NAME=$COILED_SOFTWARE_NAME_HEAD-$GITHUB_REF_TYPE-$REF_NAME_FORMATTED-$COILED_SOFTWARE_NAME_TAIL
fi
echo "Creating Coiled software environment for $COILED_SOFTWARE_NAME"
# Put COILED_SOFTWARE_NAME into $GITHUB_ENV so it can be used in subsequent workflow steps
echo COILED_SOFTWARE_NAME=$COILED_SOFTWARE_NAME >> $GITHUB_ENV
coiled env create --name $COILED_SOFTWARE_NAME --conda latest.yaml
