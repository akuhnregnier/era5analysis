#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
from datetime import timedelta

from loguru import logger

from .data import DEBUG, LOG_DIR

__all__ = ("logger",)


os.makedirs(LOG_DIR, exist_ok=True)

common_handler_config = {
    "format": (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> "
        "| <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
        "- <level>{extra[job_id]}{message}</level>"
    ),
    "enqueue": True,
}

default_filesink = {
    "rotation": timedelta(days=7),
    **common_handler_config,
}

file_handlers = []
for level in ("DEBUG", "INFO", "WARNING"):
    file_handlers.append(default_filesink.copy())
    file_handlers[-1]["level"] = level
    file_handlers[-1]["sink"] = os.path.join(
        LOG_DIR, f"era5analysis_{level.lower()}.log"
    )

config = {
    "handlers": [
        {
            "sink": sys.stderr,
            "level": "DEBUG" if DEBUG else "INFO",
            **common_handler_config,
        },
        *file_handlers,
    ],
    "extra": {"job_id": None},
    "patcher": lambda record: record["extra"].update(
        job_id=f"{record['extra']['job_id']:0>3d}:"
        if record["extra"]["job_id"] is not None
        else ""
    ),
}


logger.configure(**config)
logger.disable("era5analysis")


if __name__ == "__main__":
    logger.debug("Debug test.")
    logger.info("Info test.")
    logger.warning("Warning test.")
