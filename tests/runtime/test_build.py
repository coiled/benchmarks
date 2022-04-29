from __future__ import annotations

import json
import pathlib
import shlex
import subprocess

import coiled
import conda.cli.python_api as Conda
import dask
import pytest
import yaml
from distributed import Client
from jinja2 import Environment, FileSystemLoader, select_autoescape
from packaging.requirements import Requirement, SpecifierSet
from packaging.version import Version


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
        loader=FileSystemLoader(pathlib.Path(__file__).parent.parent.parent / "recipe"),
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

    if Version(dask.__version__).local:
        pytest.skip("Not valid on upstream build")

    meta_specifiers = get_meta_specifiers()
    installed_versions = get_conda_installed_versions()

    assert all(
        specifier.contains(installed_versions[package])
        for package, specifier in meta_specifiers.items()
    )


@pytest.mark.latest_runtime
def test_latest_coiled():
    # Ensure `coiled-runtime` installs the latest version of `coiled` by default
    v_installed = Version(coiled.__version__)

    # Get latest `coiled` release version from conda-forge
    output = subprocess.check_output(
        shlex.split("conda search --override-channels --json -c conda-forge coiled")
    )
    result = json.loads(output)
    v_latest = Version(result["coiled"][-1]["version"])

    assert v_installed == v_latest


def conda_name(package):
    if package == "msgpack":
        return "msgpack-python"
    elif package == "blosc":
        return "python-blosc"
    return package


def test_version_warning_packages():
    meta_specifiers = get_meta_specifiers()
    with Client() as client:
        info = client.get_versions()
        packages = info["client"]["packages"].keys()
        packages = [conda_name(p) for p in packages]
        assert all(p in meta_specifiers for p in packages)
