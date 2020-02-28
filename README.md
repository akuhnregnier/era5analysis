# era5analysis

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

Download and analyse ERA5 data from the Climate Data Store.

Information regarding the available variable names can be found at the [ERA5 data documentation website](https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation "ERA5: data documentation").
Either the `name` or the `shortName` columns may be used.

## Installation

Please use `conda` to install the `iris` package, as the version retrieved using `pip` is outdated.
All other dependencies should be installed automatically upon running `pip install -e .` in the repository directory.

Finally, the `config.yaml` file is required to set default values for the data and logging directories.
This should be placed in the repository directory and needs to contain the following two values, where the directories themselves may be changed, of course:
```yaml
DATA_DIR: ~/DATA/
LOG_DIR: ~/Documents/era5analysis_logs/
```
