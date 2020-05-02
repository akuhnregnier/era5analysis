# -*- coding: utf-8 -*-
import os

from iris.time import PartialDateTime

from era5analysis import (
    DATA_DIR,
    CAPEPrecipWorker,
    DailyAveragingWorker,
    logger,
    retrieval_processing,
    retrieve,
)


def monthly_averaging():
    requests = retrieve(
        variable=["2t", "10u", "10v"],
        start=PartialDateTime(1990, 1, 1),
        end=PartialDateTime(2019, 1, 1),
    )
    retrieval_processing(
        requests,
        n_threads=12,
        delete_processed=True,
        overwrite=False,
        soft_filesize_limit=3,
    )


def cape_precipitation():
    requests = retrieve(
        variable=["cape", "tp"],
        start=PartialDateTime(1990, 1, 1),
        end=PartialDateTime(2019, 1, 1),
    )
    retrieval_processing(
        requests,
        processing_class=CAPEPrecipWorker,
        n_threads=24,
        delete_processed=True,
        overwrite=False,
        soft_filesize_limit=150,
    )


def download_monthly_precipitation():
    retrieve(
        variable="tp",
        start=PartialDateTime(1990, 1, 1),
        end=PartialDateTime(2019, 1, 1),
        target_dir=os.path.join(DATA_DIR, "ERA5", "tp"),
        monthly_mean=True,
        download=True,
        merge=True,
    )
    # Not needed with `download=True`.
    # retrieval_processing(
    #     requests,
    #     processing_class=NullWorker,
    #     n_threads=12,
    #     delete_processed=False,
    #     overwrite=False,
    #     soft_filesize_limit=10,
    # )


def download_daily_precipitation():
    requests = retrieve(
        variable="tp",
        start=PartialDateTime(1990, 1, 1),
        end=PartialDateTime(2019, 1, 1),
        target_dir=os.path.join(DATA_DIR, "ERA5", "tp_daily"),
        monthly_mean=False,
        download=False,
        merge=False,
    )

    retrieval_processing(
        requests,
        processing_class=DailyAveragingWorker,
        n_threads=24,
        delete_processed=True,
        overwrite=False,
        soft_filesize_limit=250,
    )


if __name__ == "__main__":
    # To see logging messages, enable the 'era5analysis' loguru logger.
    logger.enable("era5analysis")
    cape_precipitation()
