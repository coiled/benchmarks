# If testing the latest `coiled-runtime` then install packages defined in `recipe/meta.yaml`
# Otherwise, just install directly from the coiled / conda-forge channel

set -o errexit
set -o nounset
set -o xtrace

if [[ "$COILED_RUNTIME_VERSION" =~ upstream|latest|AB_ ]]
then
  cat $1
  mamba env update --file $1
else
  mamba install -c conda-forge coiled-runtime=$COILED_RUNTIME_VERSION
fi

# For debugging
echo -e "--\n--Conda Environment (re-create this with \`conda env create --name <name> -f <output_file>\`)\n--"
mamba env export | grep -E -v '^prefix:.*$' > mamba_env_export.yml
cat mamba_env_export.yml
