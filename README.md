# era5analysis
[![License: MIT](https://img.shields.io/badge/License-MIT-blueviolet)](https://github.com/akuhnregnier/era5analysis/blob/master/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

Download and analyse ERA5 data from the Climate Data Store.

Information regarding the available variable names can be found at the [ERA5 data documentation website](https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation "ERA5: data documentation").
Either the `name` or the `shortName` columns may be used.

## Installation

First, download the repository via cloning or by downloading and then extracting the zip file.
It is assumed that the unpacked (or cloned) contents are in the `./era5analysis` directory from here on.

### Installing the Dependencies

Using `conda` with an existing, activated environment:

```sh
conda install `xargs < ./era5analysis/requirements.txt` -c conda-forge -y
```

Creating a new environment:
```sh
conda create --file ./era5analysis/requirements.txt --name era5analysis -c conda-forge -y
```
This will create a new environment called 'era5analysis' that will contain the required packages.

### Installing the `era5analysis` Package

You can install the package in editable mode like so:
```sh
pip install -e era5analysis --no-dependencies
```
Here, `--no-dependencies` is used since we have already installed the dependencies using `conda`.

### Specifying Default Directories

The data directory (`DATA_DIR`) and logging directory (should you apply the logging configuration given in the package) are both set to the current working directory by default.

The `~/era5_analysis_config.yaml` configuration file (in your __home directory__) can be used to set default values for these parameters.
This can contain the following two values, where the directories themselves may be changed, of course:

```yaml
DATA_DIR: ~/DATA/
LOG_DIR: ~/Documents/era5analysis_logs/
```

The `target_dir` argument of the `retrieve()` function takes precedence over default values for `DATA_DIR`.

## Usage

For examples, please see the following functions in [era5analysis/era5_download.py](era5analysis/era5_download.py):
 - monthly_averaging_example()
 - cape_precipitation()
 - download_monthly_precipitation()
 - download_daily_precipitation()
