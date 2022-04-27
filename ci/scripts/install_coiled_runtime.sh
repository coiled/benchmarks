# If testing the latest `coiled-runtime` then install packages defined in `recipe/meta.yaml`
# Otherwise, just install directly from the coiled / conda-forge channel

set -o errexit
set -o nounset
set -o xtrace

echo "Runtime Version is $COILED_RUNTIME_VERSION"

if [[ "$COILED_RUNTIME_VERSION" = 'latest' ]]
then
  python ci/create_latest_runtime_meta.py
  mamba install -c conda-forge --file latest.txt
else
  mamba install -c conda-forge coiled-runtime=$COILED_RUNTIME_VERSION
fi
