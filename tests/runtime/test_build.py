from __future__ import annotations

import importlib.metadata
import json
import os
import pathlib
import shlex
import subprocess
import sys

import coiled
import pytest
import yaml
from conda.models.match_spec import MatchSpec, VersionSpec
from distributed import Client
from jinja2 import Environment, FileSystemLoader, select_autoescape
from packaging.version import Version

# Note: all of these tests are local, and do not create clusters,
# so don't bother benchmarking them.


def get_installed_versions(packages) -> dict[str, str]:
    package_versions = {}
    for package in packages:
        # Python and openssl system packages
        if package == "python":
            version = ".".join(map(str, sys.version_info[:3]))
        elif package == "openssl":
            import ssl

            version = ssl.OPENSSL_VERSION.split(" ")[1]
        else:
            version = importlib.metadata.version(package)
        package_versions[package] = version

    return package_versions


def get_meta_specifiers() -> dict[str, VersionSpec]:
    """Get packages version specifiers from `meta.yaml`"""
    env = Environment(
        loader=FileSystemLoader(pathlib.Path(__file__).parent.parent.parent / "recipe"),
        autoescape=select_autoescape(),
    )
    template = env.get_template("meta.yaml")
    meta = yaml.safe_load(template.render(environ=os.environ))

    meta_specifiers = {}
    for req in meta["requirements"]["run"]:
        requirement = MatchSpec(req)
        meta_specifiers[requirement.name] = requirement.version

    return meta_specifiers


_conda_to_pip_names = {
    "msgpack-python": "msgpack",
}

_pip_to_conda_names = {v: k for k, v in _conda_to_pip_names.items()}


def pip_to_conda_name(package):
    return _pip_to_conda_names.get(package, package)


def conda_to_pip_name(package):
    return _conda_to_pip_names.get(package, package)


@pytest.mark.latest_runtime
def test_install_dist():
    # Test that versions of packages installed are consistent with those
    # specified in `meta.yaml`

    if os.environ.get("COILED_RUNTIME_VERSION", "unknown") in (
        "upstream",
        "latest",
        "unknown",
    ):
        pytest.skip("Not valid on upstream build")

    meta_specifiers = get_meta_specifiers()
    packages = [conda_to_pip_name(p) for p in meta_specifiers.keys()]
    installed_versions = get_installed_versions(packages)

    # Find and store package version mismatches
    mismatches = []
    for package in installed_versions:
        conda_name = pip_to_conda_name(package)
        specifier = meta_specifiers[conda_name]
        installed_version = installed_versions[package]
        if specifier.match(installed_version):
            continue
        else:
            mismatches.append((package, specifier, installed_version))

    if mismatches:
        raise RuntimeError(mismatches)


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


def test_version_warning_packages():
    meta_specifiers = get_meta_specifiers()
    with Client() as client:
        info = client.get_versions()
        packages = info["client"]["packages"].keys()
        packages = [pip_to_conda_name(p) for p in packages]
        assert all(p in meta_specifiers for p in packages)
