# -*- coding: utf-8 -*-
import os

import yaml

__all__ = ("DATA_DIR", "data_is_available")

config_file = os.path.join(os.path.expanduser("~"), "era5_analysis_config.yaml")

DATA_DIR = os.getcwd()
LOG_DIR = os.getcwd()
DEBUG = False

if os.path.isfile(config_file):
    with open(config_file) as f:
        config = yaml.safe_load(f)
        DATA_DIR = os.path.expanduser(config.get("DATA_DIR", DATA_DIR))
        LOG_DIR = os.path.expanduser(config.get("LOG_DIR", LOG_DIR))
        DEBUG = config.get("DEBUG", DEBUG)


def data_is_available():
    """Check if DATA_DIR exists.

    Returns:
        bool: True if the data directory exists.

    """
    return os.path.exists(DATA_DIR)
