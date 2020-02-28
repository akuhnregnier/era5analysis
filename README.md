# era5analysis

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

Download and analyse ERA5 data from the Climate Data Store.

Information regarding the available variable names can be found at the [ERA5 data documentation website](https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation "ERA5: data documentation").
Either the `name` or the `shortName` columns may be used.

## Installation

Please use `conda` to install the `iris` package, as the version retrieved using `pip` is outdated.
All other dependencies should be installed automatically upon running `pip install -e .` in the repository directory.

The data directory and logging directory (should you apply the logging configuration given in the package) are set to the current working directory by default.
Alternatively, the `era5_analysis_config.yaml` configuration file can be used to set default values for these parameters.
This should be placed in your home directory and needs to can contain the following two values, where the directories themselves may be changed, of course:
```yaml
DATA_DIR: ~/DATA/
LOG_DIR: ~/Documents/era5analysis_logs/
```

The `target_dir` argument takes precedence over both of the approaches outlined above.
