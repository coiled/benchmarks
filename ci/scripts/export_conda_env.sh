set -o errexit
set -o nounset
set -o xtrace

# For debugging
echo -e "--\n--Conda Environment (re-create this with \`conda env create --name <name> -f <output_file>\`)\n--"
# FIXME: mamba env export returns empty on CI 
conda env export | grep -E -v '^prefix:.*$'
