# -*- coding: utf-8 -*-
import os
import re

import setuptools

with open("README.md", "r") as f:
    readme = f.read()

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

here = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    with open(os.path.join(here, *parts), "r") as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setuptools.setup(
    name="era5analysis",
    version=find_version("era5analysis", "__init__.py"),
    author="Alexander Kuhn-Regnier",
    author_email="ahf.kuhnregnier@gmail.com",
    description="Download and analyse ERA5 data from the Climate Data Store.",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/akuhnregnier/era5analysis",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    include_package_data=True,
    # install_requires=requirements,
)
