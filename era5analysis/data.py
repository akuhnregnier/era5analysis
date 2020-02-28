# -*- coding: utf-8 -*-
import os

import yaml

config_file = os.path.join(os.path.expanduser("~"), "era5_analysis_config.yaml")

DATA_DIR = os.getcwd()
LOG_DIR = os.getcwd()

if os.path.isfile(config_file):
    with open(config_file) as f:
        config = yaml.safe_load(f)
        DATA_DIR = os.path.expanduser(config.get("DATA_DIR", DATA_DIR))
        LOG_DIR = os.path.expanduser(config.get("LOG_DIR", LOG_DIR))


def data_is_available():
    """Check if DATA_DIR exists.

    Returns:
        bool: True if the data directory exists.

    """
    return os.path.exists(DATA_DIR)
