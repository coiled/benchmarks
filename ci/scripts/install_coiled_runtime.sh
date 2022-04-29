# If testing the latest `coiled-runtime` then install packages defined in `recipe/meta.yaml`
# Otherwise, just install directly from the coiled / conda-forge channel

set -o errexit
set -o nounset
set -o xtrace

if [[ "$COILED_RUNTIME_VERSION" = 'latest' ]]
then
  export ENV_FILE="${ENV_FILE//<newline>/\\n}"
  echo $ENV_FILE > latest.yaml
  mamba env update --file latest.yaml
else
  mamba install -c conda-forge coiled-runtime=$COILED_RUNTIME_VERSION
fi
