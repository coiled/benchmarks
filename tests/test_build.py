import conda.cli.python_api as Conda
import yaml


def get_conda_list_dict():
    """Get conda list packages and versions in a dictionary"""
    # Get conda list packages in a list, and strip headers
    conda_list = Conda.run_command(Conda.Commands.LIST)[0].split("\n")[3:-1]
    conda_list_dict = dict()

    for package in conda_list:
        p = package.split()
        conda_list_dict[p[0]] = p[1]

    return conda_list_dict


def get_meta_packages_dict():
    """Get packages and versions specified on meta.yaml as a dictionary"""
    with open("continuous_integration/recipe/meta.yaml", "r") as f:
        meta = yaml.safe_load(f)

    meta_packages_dict = dict()

    # avoid python and pip as we don't pinned them
    for p in meta["requirements"]["run"][2:-1]:
        pv = p.split("==")
        meta_packages_dict[pv[0]] = pv[1]

    return meta_packages_dict


def test_install_dist():
    """Test that versions of packages pinned in meta match the
    versions installed"""
    d_meta = get_meta_packages_dict()
    d_conda = get_conda_list_dict()

    for p in d_meta.keys():
        assert p in d_conda
        assert d_meta[p] == d_conda[p]
