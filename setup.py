# -*- coding: utf-8 -*-

from setuptools import find_packages, setup

with open("README.md", "r") as f:
    readme = f.read()

with open("requirements.txt") as f:
    requirements = f.read().splitlines()


setup(
    name="era5analysis",
    author="Alexander Kuhn-Regnier",
    author_email="ahf.kuhnregnier@gmail.com",
    description="Download and analyse ERA5 data from the Climate Data Store.",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/akuhnregnier/era5analysis",
    package_dir={"": "src"},
    packages=find_packages("src"),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    include_package_data=True,
    # install_requires=requirements,
    extras_require={"test": ["pytest>=5.4", "pytest-cov>=2.8"]},
    setup_requires=["setuptools-scm"],
    use_scm_version=dict(write_to="src/era5analysis/_version.py"),
)
