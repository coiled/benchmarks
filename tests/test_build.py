import pathlib

import conda.cli.python_api as Conda
import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape


def get_conda_list_dict():
    # Get conda list packages in a list, and strip headers
    conda_list = Conda.run_command(Conda.Commands.LIST)[0].split("\n")[3:-1]
    conda_list_dict = dict()

    for package in conda_list:
        p = package.split()
        conda_list_dict[p[0]] = p[1]

    return conda_list_dict


def get_meta_packages_dict():
    """Get packages and versions specified on meta.yaml as a dictionary"""
    env = Environment(
        loader=FileSystemLoader(
            pathlib.Path(__file__).parent.parent / "continuous_integration/recipe"
        ),
        autoescape=select_autoescape(),
    )
    template = env.get_template("meta.yaml")
    meta = yaml.safe_load(template.render())

    meta_versions = {}

    # avoid checking unpinned packages
    for p in meta["requirements"]["run"]:
        if " ==" in p:
            pv = p.split(" ==")
            meta_versions[pv[0]] = pv[1]

    return meta_versions


def test_install_dist():
    """Test that versions of packages pinned in meta match the
    versions installed"""
    d_meta = get_meta_packages_dict()
    d_conda = get_conda_list_dict()
    for p in d_meta.keys():
        assert p in d_conda
        assert d_meta[p] == d_conda[p]
