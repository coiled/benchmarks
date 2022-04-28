set -o errexit
set -o nounset
set -o xtrace

# Clean up an Coiled software environments we created just for this CI build
export COILED_SOFTWARE_NAME=${{ env.COILED_SOFTWARE_NAME }}
coiled env delete $COILED_SOFTWARE_NAME
if [[ ${{ steps.runtime_tests.outcome }} != 'success' ]]
then
python -c "raise Exception('Tests failed. Please see the output from the previous step for more details.')"
fi
