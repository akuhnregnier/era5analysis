#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging.config
import os
from copy import deepcopy

from .data import LOG_DIR as log_dir

if not os.path.isdir(log_dir):
    os.makedirs(log_dir)

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": (
                "%(asctime)s:%(levelname)-8s:%(name)-20s:%(filename)-25s:%(lineno)-5s"
                ":%(funcName)-30s:%(message)s"
            )
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "default",
        },
        "debug_file": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "when": "D",
            "interval": 7,
            "level": "DEBUG",
            "formatter": "default",
            "filename": os.path.join(log_dir, "wildfires_debug.log"),
        },
        "info_file": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "when": "D",
            "interval": 7,
            "level": "INFO",
            "formatter": "default",
            "filename": os.path.join(log_dir, "wildfires_info.log"),
        },
        "warning_file": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "when": "D",
            "interval": 7,
            "level": "WARNING",
            "formatter": "default",
            "filename": os.path.join(log_dir, "wildfires_warning.log"),
        },
        "root_debug_file": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "when": "D",
            "interval": 7,
            "level": "DEBUG",
            "formatter": "default",
            "filename": os.path.join(log_dir, "root_debug.log"),
        },
        "root_info_file": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "when": "D",
            "interval": 7,
            "level": "INFO",
            "formatter": "default",
            "filename": os.path.join(log_dir, "root_info.log"),
        },
        "root_warning_file": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "when": "D",
            "interval": 7,
            "level": "WARNING",
            "formatter": "default",
            "filename": os.path.join(log_dir, "root_warning.log"),
        },
    },
    "loggers": {
        "wildfires": {
            "level": "DEBUG",
            "handlers": ["console", "debug_file", "info_file", "warning_file"],
        },
        "": {
            "level": "DEBUG",
            "handlers": ["root_debug_file", "root_info_file", "root_warning_file"],
        },
    },
}

LOGGING["loggers"]["__main__"] = LOGGING["loggers"]["wildfires"]
# A copy of the usual configuration with a higher threshold for console output.
JUPYTER_LOGGING = deepcopy(LOGGING)
JUPYTER_LOGGING["handlers"]["console"]["level"] = "WARNING"


def enable_logging(mode="normal", level=None):
    """Configure logging in a standardised manner.

    Args:
        mode (str): Which configuration to use. Possible values are "normal" or
            "jupyter".
        level (logging level): If given, alter the console logger level.

    """
    if mode == "normal":
        if level is not None:
            LOGGING["handlers"]["console"]["level"] = level
        logging.config.dictConfig(LOGGING)
    elif mode == "jupyter":
        if level is not None:
            JUPYTER_LOGGING["handlers"]["console"]["level"] = level
        logging.config.dictConfig(JUPYTER_LOGGING)
    else:
        raise ValueError(f"Unknown mode '{mode}'.")


if __name__ == "__main__":
    import logging
    from logging_tree import printout
    import cdsapi

    c = cdsapi.Client()
    logging.config.dictConfig(LOGGING)
    logger0 = logging.getLogger("")
    logger1 = logging.getLogger(__name__)
    logger2 = logging.getLogger("testing")
    printout()

    for level, level_str in (
        (logging.DEBUG, "DEBUG"),
        (logging.INFO, "INFO"),
        (logging.WARNING, "WARNING"),
    ):
        for logger, logger_name in (
            (logger0, "root logger"),
            (logger1, "{} logger".format(__name__)),
            (logger2, "testing logger"),
        ):
            logger.log(level, "{} {}".format(level_str, logger_name + " test"))
