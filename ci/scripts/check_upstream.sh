set -o errexit
set -o nounset
set -o xtrace

COMMIT="$(git log -n 1 --pretty=format:%s HEAD^2)"
if [[ "$COMMIT" == *"test-upstream"* ]]
then
export TEST_UPSTREAM="true"
# Put TEST_UPSTREAM into $GITHUB_ENV so it can be used in subsequent workflow steps
echo TEST_UPSTREAM=$TEST_UPSTREAM >> $GITHUB_ENV
echo "Using development version of dask and distributed"
fi
