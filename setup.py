#!/usr/bin/env python

import os
import pathlib

import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape
from packaging.requirements import Requirement
from setuptools import setup


def get_requirements():
    """Get package requirements from conda recipe meta.yaml file"""
    env = Environment(
        loader=FileSystemLoader(pathlib.Path(__file__).parent / "recipe"),
        autoescape=select_autoescape(),
    )
    template = env.get_template("meta.yaml")
    meta = yaml.safe_load(template.render(environ=os.environ))

    requirements = {}
    for req in meta["requirements"]["run"]:
        requirement = Requirement(req)
        requirements[requirement.name] = str(requirement.specifier)

    # Handle packages that have different names on conda-forge and PyPI
    requirements["blosc"] = requirements.pop("python-blosc")
    requirements["msgpack"] = requirements.pop("msgpack-python")

    # Get Python version requirements (also not included on PyPI)
    python_requires = requirements.pop("python")

    install_requires = [
        f"{name}{specifier}" for name, specifier in requirements.items()
    ]

    return python_requires, install_requires


python_requires, install_requires = get_requirements()

setup(
    name="coiled-runtime",
    version="0.1.0",
    description="Simple and fast way to get started with Dask",
    url="https://github.com/coiled/coiled-runtime",
    license="BSD",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: BSD License",
    ],
    packages=["coiled_runtime"],
    long_description=open("README.md").read(),
    python_requires=python_requires,
    install_requires=install_requires,
)
