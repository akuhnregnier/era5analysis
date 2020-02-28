# -*- coding: utf-8 -*-
import os

import yaml

config_file = os.path.join(os.path.dirname(__file__), os.pardir, "config.yaml")

DATA_DIR = None
LOG_DIR = None

if os.path.isfile(config_file):
    with open(config_file) as f:
        config = yaml.safe_load(f)
        DATA_DIR = os.path.expanduser(config["DATA_DIR"])
        LOG_DIR = os.path.expanduser(config["LOG_DIR"])
else:
    raise FileNotFoundError(f"Please populate the config file {config_file}.")


def data_is_available():
    """Check if DATA_DIR exists.

    Returns:
        bool: True if the data directory exists.

    """
    return os.path.exists(DATA_DIR)
