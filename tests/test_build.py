from __future__ import annotations

import pathlib

import conda.cli.python_api as Conda
import pytest
import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape
from packaging.requirements import Requirement, SpecifierSet


def get_conda_installed_versions() -> dict[str, str]:
    """Get conda list packages in a list, and strip headers"""
    conda_list = Conda.run_command(Conda.Commands.LIST)[0].split("\n")[3:-1]

    package_versions = {}
    for line in conda_list:
        name, version, *_ = line.split()
        package_versions[name] = version

    return package_versions


def get_meta_specifiers() -> dict[str, SpecifierSet]:
    """Get packages version specifiers from `meta.yaml`"""
    env = Environment(
        loader=FileSystemLoader(pathlib.Path(__file__).parent.parent / "recipe"),
        autoescape=select_autoescape(),
    )
    template = env.get_template("meta.yaml")
    meta = yaml.safe_load(template.render())

    meta_specifiers = {}
    for req in meta["requirements"]["run"]:
        requirement = Requirement(req)
        meta_specifiers[requirement.name] = requirement.specifier

    return meta_specifiers


@pytest.mark.latest_runtime
def test_install_dist():
    # Test that versions of packages installed are consistent with those
    # specified in `meta.yaml`
    meta_specifiers = get_meta_specifiers()
    installed_versions = get_conda_installed_versions()

    assert all(
        specifier.contains(installed_versions[package])
        for package, specifier in meta_specifiers.items()
    )
