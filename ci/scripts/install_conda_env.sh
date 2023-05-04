set -o errexit
set -o nounset
set -o xtrace

cat $1
mamba env update --file $1

# For debugging
echo -e "--\n--Conda Environment (re-create this with \`conda env create --name <name> -f <output_file>\`)\n--"
mamba env export | grep -E -v '^prefix:.*$' > mamba_env_export.yml
cat mamba_env_export.yml
