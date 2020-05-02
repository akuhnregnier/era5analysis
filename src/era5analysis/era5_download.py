# -*- coding: utf-8 -*-
"""Tools for downloading ERA5 data using the CDS API."""
import calendar
import json
import multiprocessing
import os
import sys
from copy import deepcopy
from datetime import datetime
from multiprocessing import Pipe, Queue
from threading import Thread
from time import sleep, time

import cdsapi
import numpy as np
from dateutil.relativedelta import relativedelta
from iris.time import PartialDateTime

from .data import DATA_DIR, LOG_DIR
from .era5_tables import get_long_to_short
from .logging_config import logger
from .processing_workers import *

__all__ = [
    "ThreadList",
    "format_request",
    "format_variable",
    "retrieval_processing",
    "retrieve",
    "str_to_seconds",
]


SINGLE_LEVEL_DATASET = "reanalysis-era5-single-levels"
PRESSURE_LEVEL_DATASET = "reanalysis-era5-pressure-levels"
SINGLE_LEVEL_MEAN_DATASET = "reanalysis-era5-single-levels-monthly-means"
PRESSURE_LEVEL_MEAN_DATASET = "reanalysis-era5-pressure-levels-monthly-means"

SINGLE_LEVEL_NAMES = (SINGLE_LEVEL_DATASET, SINGLE_LEVEL_MEAN_DATASET)
PRESSURE_LEVEL_NAMES = (PRESSURE_LEVEL_DATASET, PRESSURE_LEVEL_MEAN_DATASET)
MEAN_DATASET_NAMES = (SINGLE_LEVEL_MEAN_DATASET, PRESSURE_LEVEL_MEAN_DATASET)

long_to_short = get_long_to_short()


def is_single_level_dataset(dataset_name):
    return dataset_name in SINGLE_LEVEL_NAMES


def is_pressure_level_dataset(dataset_name):
    return dataset_name in PRESSURE_LEVEL_NAMES


def is_mean_dataset(dataset_name):
    return dataset_name in MEAN_DATASET_NAMES


def format_request(request):
    """Format the request tuple for nicer printing.

    Returns:
        str: Formatted request.

    """
    request = deepcopy(request)
    request_dict = request[1]

    if request_dict["product_type"] == "reanalysis":
        days = []
        for year in request_dict["year"]:
            for month in request_dict["month"]:
                n_days = calendar.monthrange(int(year), int(month))[1]
                days.append(n_days)

        if len(request_dict["day"]) == max(days):
            day_str = "ALL"
        else:
            day_str = ", ".join(request_dict["day"])

        if len(request_dict["time"]) == 24:
            time_str = "ALL"
        else:
            time_str = ", ".join(request_dict["time"])

        if len(request_dict["month"]) == 12:
            month_str = "ALL"
        else:
            month_str = ", ".join(
                calendar.month_abbr[int(month)] for month in request_dict["month"]
            )

        year_str = ", ".join(request_dict["year"])

        output = "{} from {} for year(s) {}, month(s) {}, day(s) {}, time(s) {}.".format(
            request_dict["variable"], request[0], year_str, month_str, day_str, time_str
        )
    elif request_dict["product_type"] == "monthly_averaged_reanalysis":
        if len(request_dict["month"]) == 12:
            month_str = "ALL"
        else:
            month_str = ", ".join(
                calendar.month_abbr[int(month)] for month in request_dict["month"]
            )

        year_str = ", ".join(request_dict["year"])

        output = "{} from {} for year(s) {}, month(s) {}.".format(
            request_dict["variable"], request[0], year_str, month_str
        )

    else:
        raise ValueError(
            "Unknown product type '{}'.".format(request_dict["product_type"])
        )
    return output


def str_to_seconds(string):
    """Pocesses a string including time units into a float in seconds.

    Args:
        string (str): Input string, including units.

    Returns:
        float: The processed time in seconds.

    Examples:
        >>> int(round(str_to_seconds('1')))
        1
        >>> int(round(str_to_seconds('1s')))
        1
        >>> int(round(str_to_seconds('2m')))
        120
        >>> int(round(str_to_seconds('3h')))
        10800
        >>> int(round(str_to_seconds('2d')))
        172800

    """
    if isinstance(string, str):
        multipliers = {"s": 1.0, "m": 60.0, "h": 60.0 ** 2.0, "d": 24.0 * 60 ** 2.0}
        for key, multiplier in zip(multipliers, list(multipliers.values())):
            if key in string:
                return float(string.strip(key)) * multiplier
    return float(string)


def format_variable(
    variable,
    variable_name,
    single_value_formatter=str,
    valid_single_types=(str, float, np.float, int, np.integer),
):
    """Format a variable consistently.

    Args:
        variable (one of 'valid_single_types' or a list of
            'valid_single_types'): The variable to format.
        variable_name (str): The name of the variable. Only relevant for
            error messages.
        single_value_formatter (callable): Function called for each single
            variable in 'variable'.
        valid_single_types (tuple of types): Types which are handled
            correctly by the 'single_value_formatter' callable.

    Returns:
        list: A list (containing one or more elements) of the formatted
            variables.

    Raises:
        TypeError: If one of the elements of 'variable' do not match the
            'valid_single_types'.

    """

    if isinstance(variable, valid_single_types):
        return [single_value_formatter(variable)]

    type_error_msg = (
        f"Type '{type(variable)}' not supported for argument '{variable_name}'."
    )
    if hasattr(variable, "__iter__"):
        formatted_variables = []
        for single_value in variable:
            if not isinstance(single_value, valid_single_types):
                raise TypeError(type_error_msg)
            formatted_variables.append(single_value_formatter(single_value))
        return formatted_variables
    raise TypeError(type_error_msg)


def retrieve(
    variable="2m_temperature",
    levels="sfc",
    hours=None,
    monthly_mean=False,
    start=PartialDateTime(2000, 1, 1),
    end=PartialDateTime(2000, 2, 1),
    target_dir=os.path.join(DATA_DIR, "ERA5"),
    download=False,
    merge=False,
):
    """Retrieve hourly ERA5 data for the chosen variable.

    Possible values for the variable parameter can be taken from the short names
    (shortName column) in the tables at
    https://confluence.ecmwf.int/display/CKB/ERA5+data+documentation.

    Note:
        Variables may have different names depending on whether the 'sfc'
        level or a pressure level is requested.

        Time information (ie. hours, minutes, etc...) in the start and end
        arguments will be ignored.

    TODO:
        Giving `levels` as an iterable should be supported by making the application
        of `.lower()` and following string comparison more flexible.

    Args:
        variable (str or list of str): Variable of interest: eg.
            variable='2t' or variable='2m_temperature' refers to
            the 2m surface (sfc) level temperature variable. Multiple
            variables are also possible if given as a list: eg.
            variable=['2t', '10u']  would retrieve 2m temperature and
            10 m U wind component.
        levels (str, int, or list of str or int): If levels='sfc', the
            surface data will be requested. Alternatively, levels='100',
            levels='100 hPa' or levels=100 would all select the 100 hPa
            level. Level values can also be put be given as a list.
        hours (None, str, int, or list of str or int): If hours=None,
            retrieve all hours. Alternatively hours may be given as
            integers (eg. hours=1), strings (eg. hours='01:00') or as lists
            of these. The hours must be in the range [0, 23].
        monthly_mean (bool): If `monthly_mean`, retrieve monthly means instead of
            hourly data.
        start (datetime): Initial datetime. This is inclusive (see 'end').
        end (datetime): Final datetime. This is not inclusive. So
            start=PartialDateTime(2000, 1, 1), end=PartialDateTime(2000, 2, 1)
            will retrieve all data for January.
        target_dir (str): Directory path where the output files will be stored.
        download (bool): If True, download data one requests at a time. If
            False, simply return the list of request tuples that can be
            used to download data (e.g. using `retrieval_processing`).
        merge (bool): If `merge`, issue a single request instead of individual monthly
            requests across the specified time interval. This may lead to more data
            being downloaded than specified, however, as all specified months will be
            applied to all years in the interval.

    Returns:
        list: list of request tuples. Each tuple contains the dataset
            string, the request body as a dictionary, and the filename as a
            string. There will be one output filename per month containing
            all of the requested variables, named like
            era5_hourly_reanalysis_{year}_{month}.nc.

    """
    if download:
        client = cdsapi.Client(quiet=True)
    else:
        client = None

    if isinstance(variable, str):
        variable = [variable]

    # Convert variables to the short format.
    variable = [long_to_short.get(var, var) for var in variable]

    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)

    if levels.lower() in ("sfc", "surface"):
        logger.debug("Retrieving surface dataset.")
        if monthly_mean:
            dataset = SINGLE_LEVEL_MEAN_DATASET
        else:
            dataset = SINGLE_LEVEL_DATASET
    else:
        logger.debug("Retrieving pressure level dataset.")
        if monthly_mean:
            dataset = PRESSURE_LEVEL_MEAN_DATASET
        else:
            dataset = PRESSURE_LEVEL_DATASET

    if hours is None:
        if is_mean_dataset(dataset):
            hours = "00:00"
        else:
            hours = ["{:02d}:00".format(hour) for hour in range(0, 24)]
    else:

        def single_hour_formatter(hour):
            if isinstance(hour, str):
                if ":" in hour:
                    hour = hour[: hour.find(":")]
                else:
                    assert (
                        len(hour) <= 2
                    ), "'hours' written like '{}' are not supported.".format(hour)
                hour = int(hour)
            elif isinstance(hour, (float, np.float)):
                hour = round(hour)
            # No else statement here is needed due to the types given to
            # the 'format_variable' function via 'valid_single_types'.
            return "{:02d}:00".format(hour)

        hours = format_variable(
            hours,
            "hours",
            single_hour_formatter,
            (str, float, np.float, int, np.integer),
        )
    logger.debug("Request hours: '{}'.".format(hours))

    # 'levels' is only relevant for the pressure level dataset.
    if is_pressure_level_dataset(dataset):

        def level_formatter(level):
            if isinstance(level, str):
                # Remove 'hPa'. Trailing space would be stripped int().
                level = int(level.lower().strip("hpa"))
            elif isinstance(level, (float, np.float)):
                level = round(level)
            return str(level)

        levels = format_variable(
            levels, "levels", level_formatter, (str, float, np.float, int, np.integer)
        )
    logger.debug("Request levels:{}.".format(levels))

    # Accumulate the date strings.
    monthly_dates = dict()
    start_date = datetime(start.year, start.month, start.day)
    current_date = start_date
    end_date = datetime(end.year, end.month, end.day)

    # Since the date given for 'end' is not inclusive.
    dates = []
    while current_date != end_date:
        dates.append(current_date)
        current_month_date = (current_date.year, current_date.month)
        if current_month_date not in monthly_dates:
            monthly_dates[current_month_date] = {
                "year": [str(current_date.year)],
                "month": ["{:02d}".format(current_date.month)],
                "day": ["{:02d}".format(current_date.day)],
            }
        else:
            monthly_dates[current_month_date]["day"].append(
                "{:02d}".format(current_date.day)
            )
        current_date += relativedelta(days=+1)

    requests = []
    if not merge:
        for request_date in monthly_dates.values():
            logger.debug("Request dates: '{}'.".format(request_date))
            request_dict = {
                "format": "netcdf",
                "variable": variable,
                "year": request_date["year"],
                "month": request_date["month"],
                "time": hours,
            }

            if is_mean_dataset(dataset):
                request_dict["product_type"] = "monthly_averaged_reanalysis"
                filename = "era5_hourly_reanalysis_monthly_mean_{year}_{month}.nc".format(
                    year=request_dict["year"][0], month=request_dict["month"][0]
                )
            else:
                request_dict["day"] = request_date["day"]
                request_dict["product_type"] = "reanalysis"
                filename = "era5_hourly_reanalysis_{year}_{month}.nc".format(
                    year=request_dict["year"][0], month=request_dict["month"][0]
                )

            if is_pressure_level_dataset(dataset):
                request_dict["pressure_level"] = levels

            target_file = os.path.join(target_dir, filename)

            request = (dataset, request_dict, target_file)
            requests.append(request)
    else:
        logger.debug("Creating single request.")
        request_dict = {
            "format": "netcdf",
            "variable": variable,
            "year": list(map(str, sorted(list(set(dt.year for dt in dates))))),
            "month": list(
                map("{:02d}".format, sorted(list(set(dt.month for dt in dates))))
            ),
            "time": hours,
        }

        if is_mean_dataset(dataset):
            request_dict["product_type"] = "monthly_averaged_reanalysis"
            filename = "era5_hourly_reanalysis_monthly_mean_{year}_{month}.nc".format(
                year=request_dict["year"][0], month=request_dict["month"][0]
            )
        else:
            request_dict["day"] = list(
                map("{:02d}".format, sorted(list(set(dt.day for dt in dates))))
            )
            request_dict["product_type"] = "reanalysis"
            filename = "era5_hourly_reanalysis_{year}_{month}.nc".format(
                year=request_dict["year"][0], month=request_dict["month"][0]
            )

        if is_pressure_level_dataset(dataset):
            request_dict["pressure_level"] = levels

        target_file = os.path.join(target_dir, filename)

        request = (dataset, request_dict, target_file)
        requests.append(request)

    if download:
        for dataset, request_dict, target_file in requests:
            if not os.path.isfile(target_file):
                logger.info("Starting download to: '{}'.".format(target_file))
                client.retrieve(dataset, request_dict, target_file)
                logger.info("Finished download to: '{}'.".format(target_file))
            else:
                logger.info(
                    f"File already exists, not starting download to: '{target_file}'."
                )

    return requests


class DownloadThread(Thread):
    """Retrieve data using the CDS API.

    DownloadThread.queue is the Queue shared by all DownloadThread instances
    to communicate what they have finished downloading.

    DownloadThread.event is the Event shared by all DownloadThread instances
    to communicate when they have finished downloading their request.
    """

    id_index = 1

    def __init__(self, request, *args, **kwargs):
        assert hasattr(self, "queue"), "Must call assign_queue."
        assert hasattr(self, "event"), "Must call assign_event."
        super().__init__(*args, **kwargs)
        self.id_index = DownloadThread.id_index
        self.request = request
        self.formatted_request = format_request(self.request)

        # Configure a logger using `id_index`.
        self.logger = logger.bind(job_id=DownloadThread.id_index)

        # Need quiet=True, because otherwise the initialisation of Client
        # will call logging.basicConfig, which modifies the root logger and
        # results in duplicated logging messages with our current setup.
        self.client = cdsapi.Client(quiet=True)
        self.logger.debug(
            "Initialised DownloadThread with id_index={}.".format(self.id_index)
        )
        DownloadThread.id_index += 1

    @classmethod
    def assign_queue(cls, queue):
        """Assign a Queue to the class for shared usage.

        Args:
            queue (multiprocessing.queues.Queue): Shared Queue.

        """
        assert isinstance(queue, multiprocessing.queues.Queue)
        cls.queue = queue
        return cls

    @classmethod
    def assign_event(cls, event):
        """Assign an Event to the class for shared usage.

        Args:
            queue (multiprocessing.synchronize.Event): Shared Event.

        """
        assert isinstance(event, multiprocessing.synchronize.Event)
        cls.event = event
        return cls

    @staticmethod
    def clear_request_log_file(request):
        """Remove the request log file associated with the request.

        If the log file does not exist, nothing is done.

        """
        log_file = DownloadThread.get_request_log_file(request)
        if os.path.isfile(log_file):
            logger.debug("Removed request log file: '{}'.".format(log_file))
            os.remove(log_file)

    @staticmethod
    def get_request_log_file(request):
        """Get the request log filename associated with the request.

        Returns:
            str: Request log filename.

        """
        logger.debug("Getting request log file for data file: '{}'".format(request[2]))
        orig_dir, orig_filename = os.path.split(request[2])
        request_log_file = os.path.join(
            orig_dir, ".requests", ".".join(orig_filename.split(".")[:-1] + ["request"])
        )
        return request_log_file

    @staticmethod
    def retrieve_request(request):
        """Retrieve the original request associated with the target
        filename in the request.

        """
        try:
            request_log_file = DownloadThread.get_request_log_file(request)
            with open(request_log_file, "r") as f:
                stored_request = tuple(json.load(f))
            return stored_request
        except FileNotFoundError:
            logger.warning(
                "Request log could not be found for the following file: {}.".format(
                    request[2]
                )
            )
            return (None, None, None)

    def record_request(self):
        request_log_file = self.get_request_log_file(self.request)
        if not os.path.isdir(os.path.dirname(request_log_file)):
            os.makedirs(os.path.dirname(request_log_file))
        self.logger.debug("Recording request in file '{}'".format(request_log_file))
        with open(request_log_file, "w") as f:
            json.dump(self.request, f, indent=4)

    def run(self):
        try:
            self.logger.info("Requesting: {}".format(self.formatted_request))
            # In case the original file was deleted without deleting the
            # associated request. Without this step, an interrupted download
            # would not be detected.
            DownloadThread.clear_request_log_file(self.request)
            self.client.retrieve(*self.request)
            self.logger.debug("Completed request.")
            filename = self.request[2]
            if not os.path.isfile(filename):
                raise RuntimeError(
                    "Filename '{}' not found despite request "
                    "'{}' having being issued.".format(filename, self.request)
                )
            self.record_request()
            self.queue.put(self.request)
            self.logger.debug("Setting event flag.")
            self.event.set()
        except Exception:
            self.queue.put(sys.exc_info())
        finally:
            self.logger.debug("Exiting.")


class ThreadList:
    """A list of Thread instances."""

    def __init__(self, threads=None):
        if threads is None:
            threads = []
        self.threads = threads

    def __iter__(self):
        return iter(self.threads)

    def __len__(self):
        return len(self.threads)

    def append(self, thread):
        self.threads.append(thread)

    def prune(self):
        """Remove stopped threads in-place.

        Threads whose is_alive() method return False are removed.

        """
        to_remove = []
        for thread in self.threads:
            if not thread.is_alive():
                logger.info("DownloadThread {} has finished.".format(thread.id_index))
                to_remove.append(thread)
        for completed_thread in to_remove:
            completed_thread.join(1.0)
            self.threads.remove(completed_thread)

    @property
    def n_alive(self):
        """Return the number of alive threads.

        Returns:
            int: Number of threads that are alive.

        """
        counter = 0
        for thread in self.threads:
            if thread.is_alive():
                counter += 1
        return counter


def new_download_thread(new_request, overwrite, processing_class):
    """Start a new download thread given conditions.

    Args:
        new_request (tuple): A request tuple as returned by `retrieve`.
        overwrite (bool): If True, overwrite existing data.
        processing_class (class): Subclass of `Worker`, responsible for
            processing downloaded data. See `processing_class` in
            `retrieval_processing`.

    Returns:
        Thread or None: None is returned if there is no data to download. None
            is not returned if overwrite is True.

    """
    new_filename = new_request[2]
    output_filename = processing_class.output_filename(new_filename)
    expected_output = (0, new_filename, output_filename)

    if not overwrite and os.path.isfile(output_filename):
        if processing_class.check_output(new_request, expected_output):
            logger.info(
                "'{}' already contains correct processed data. "
                "Not downloading raw data.".format(output_filename)
            )
            return None
    if not overwrite and os.path.isfile(new_filename):
        # Don't keep last item in comparison so that this still matches in
        # case the source files were moved.
        if DownloadThread.retrieve_request(new_request)[:2] != new_request[:2]:
            DownloadThread.clear_request_log_file(new_request)
            logger.warning(
                "'{}' contains raw data for another request. "
                "Deleting this file and retrieving new request.".format(new_filename)
            )
            os.remove(new_filename)
            return DownloadThread(new_request)

        logger.warning(
            "'{}' contains raw data for this request, but correct processed "
            "data was not found at '{}'. Sending request for processing now. "
            "If processing fails (see error logs at '{}') the raw data should "
            "be deleted and downloaded again.".format(
                new_filename, output_filename, LOG_DIR
            )
        )
        # Taking a shortcut here - instead of spawning a new DownloadThread
        # and downloading the file, emulate the behaviour of a DownloadThread
        # by sending the request (for which there is already data, as
        # required) to the queue normally used by the DownloadThread
        # instances, thereby signalling that the file has been successfully
        # 'downloaded', i.e. it is available for processing.
        DownloadThread.queue.put(new_request)
        DownloadThread.event.set()
        # It takes some time for the queue to register this. Allow up to 0.5
        # seconds for this to happen (just to be sure).
        check_start = time()
        while time() - check_start < 0.5:
            if not DownloadThread.queue.empty():
                break
            sleep(0.01)
        else:
            assert not DownloadThread.queue.empty(), "The queue should not be empty."
        return None

    return DownloadThread(new_request)


def retrieval_processing(
    requests,
    processing_class=AveragingWorker,
    n_threads=4,
    soft_filesize_limit=1000,
    timeout="3d",
    delete_processed=True,
    overwrite=False,
):
    """Start retrieving and processing data asynchronously.

    The calling process spawns one non-blocking process just for the
    processing of the downloaded files. This process is fed with the
    filenames of the files as they are downloaded by the download threads.
    It then proceeds to average these values (without blocking the download
    of the other data or blocking).

    A number of threads (`n_threads`) will be started in order to download
    data concurrently. If one thread finishes downloading data, it will
    receive a new request to retrieve, and so on for the other threads,
    until all the requests have been handled.

    The main process checks the output of these download threads, and if a
    new file has been downloaded successfully, it is added to the
    processing queue for the distinct processing process.

    Note:
        Using NetCDF and a 0.25 x 0.25 grid, each variable takes up ~1.5 GB
        per pressure level per month.

    Args:
        requests (list): A list of 3 element tuples which are passed to the
            retrieve function of the CDS API client in order to retrieve
            the intended data.
        processing_class (`Worker`): A subclass of `Worker` that
            defines a process method and takes and index (int) and a pipe
            (multiprocessing.connection.Connection) as constructor
            arguments. See `AveragingWorker` for a sample implementation.
        n_threads (int): The maximum number of data download threads to
            open at any one time. This corresponds to the number of open
            requests to the CDS API.
        soft_filesize_limit (float): If the cumulative size of downloaded
            and processed files on disk (see `delete_processed`) in GB
            exceeds this value, downloading of new files (ie. spawning of
            new threads) will cease until the aforementioned cumulative
            size drops below the threshold again. Exceeding the threshold
            does not terminate existing download threads or the processing
            worker, meaning that at most `n_threads - 1` downloads and
            processing of files in the worker queue could still occur
            before requests and processing are ceased entirely. If None, no
            limit is applied. Note that this does not consider pre-existing
            files in the target directory.
        timeout (float or str): Time-out after which the function
            terminates and ceases downloads as well as processing. If None, no
            limit is applied. If given as a float, the units are in
            seconds. A string may be given, in which case the units may be
            dictated. For example, '1s' would refer to one second, '1m' to
            one minute, '1h' to one hour, and '2d' to two days.
        delete_processed (bool): If True, remove downloaded files that have
            been successfully processed.
        overwrite (bool): If True, download files which have already been
            downloaded again. Note that only the time coordinate and
            variables are compared against for the existing files.

    TODO:
        Soft time-out which would not cause an abrupt exit of the program
        (using an Exception) but would allow a graceful exit.

        Check that downloaded files have correct grid (not relevant if grid
        is not specified and only the default is retrieved).

        Check that downloaded files have the correct units, which can be
        achieved by using wildfires.data.era5_tables.get_table_dict, as it
        returns a dictionary mapping long variable names to (amongst
        others) units.

    """
    start_time = time()
    if isinstance(timeout, str):
        timeout = str_to_seconds(timeout)
    requests = requests.copy()
    threads = ThreadList()
    remaining_files = []
    total_files = []
    processed_files = []
    issued_filesize_warning = False
    if delete_processed:
        raw_files = remaining_files
    else:
        raw_files = total_files

    event = multiprocessing.Event()

    pipe_start, pipe_end = Pipe()
    processing_class.assign_event(event)
    processing_worker = processing_class(0, pipe_end)
    processing_worker.start()

    retrieve_queue = Queue()
    DownloadThread.assign_event(event)
    DownloadThread.assign_queue(retrieve_queue)

    size_limit_msg = (
        "Soft file size limit {}exceeded. Requested limit: {:0.1e} GB. "
        "Observed: {:0.1e} GB. Pending downloads: {}."
    )

    while requests or remaining_files or threads:
        time_taken = time() - start_time
        if time_taken > timeout:
            error_str = "Timeout of {:0.1e} s exceeded by {:0.1e} s.".format(
                timeout, time_taken - timeout
            )
            raise RuntimeError(error_str)
        threads.prune()
        logger.info("Remaining files to process: {}.".format(len(remaining_files)))
        logger.debug("Remaining files to process: {}.".format(remaining_files))
        logger.info(
            "Number of remaining requests to process: {}.".format(len(requests))
        )

        # Retrieve new requests by spawning new threads.
        new_threads = []
        check_files = raw_files + processed_files
        filesize_sum = sum([os.path.getsize(f) for f in check_files]) / 1000 ** 3
        if filesize_sum > soft_filesize_limit:
            logger.warning(
                size_limit_msg.format(
                    "", soft_filesize_limit, filesize_sum, threads.n_alive
                )
            )
            issued_filesize_warning = True
        else:
            if issued_filesize_warning:
                issued_filesize_warning = False
                logger.warning(
                    size_limit_msg.format(
                        "no longer ", soft_filesize_limit, filesize_sum, len(threads)
                    )
                )
            while len(threads) < n_threads and requests:
                new_request = requests.pop()
                new_thread = new_download_thread(
                    new_request, overwrite, processing_class
                )
                if new_thread is None:
                    logger.debug("No new download requested.")
                    continue
                # These threads will die when the main program exits.
                new_thread.daemon = True
                new_threads.append(new_thread)
                threads.append(new_thread)

        logger.debug("Starting {} new thread(s).".format(len(new_threads)))
        for new_thread in new_threads:
            new_thread.start()
        logger.debug("Number of threads: {}.".format(len(threads)))
        logger.info("Pending downloads: {}.".format(threads.n_alive))

        # event.set() will never be called if there are no pending downloads
        # and files pending processing.
        if not threads and not remaining_files and retrieve_queue.empty():
            break

        logger.debug(
            "Waiting for download or processing for {:0.1e} seconds.".format(timeout)
        )

        event.wait(timeout)
        event.clear()

        finished_downloads = not retrieve_queue.empty()
        logger.debug("Finished downloads: {}.".format(not finished_downloads))
        # Handle all downloaded requests.
        while not retrieve_queue.empty():
            retrieve_output = retrieve_queue.get()

            logger.debug("Retrieval output: {}.".format(retrieve_output))
            # The output may contain sys.exc_info().
            if (
                hasattr(retrieve_output, "__len__")
                and len(retrieve_output) > 1
                and isinstance(retrieve_output[1], Exception)
            ):
                # Re-raise exception here complete with traceback, and log it.
                try:
                    raise retrieve_output[1].with_traceback(retrieve_output[2])
                except Exception:
                    logger.exception("Exception while downloading data.")
                    continue
            # If it is a filename and not an exception, add this to the
            # queue for the processing worker.
            logger.debug("Sending filename to worker: '{}'.".format(retrieve_output[2]))
            pipe_start.send(retrieve_output)
            remaining_files.append(retrieve_output[2])
            total_files.append(retrieve_output[2])

        # Handle all processed requests.
        while pipe_start.poll():
            output = pipe_start.recv()
            # The output may contain sys.exc_info().
            if (
                hasattr(output, "__len__")
                and len(output) > 1
                and isinstance(output[1], Exception)
            ):
                # Re-raise exception here complete with traceback, and log it.
                try:
                    raise output[1].with_traceback(output[2])
                except Exception:
                    logger.exception("Exception while processing data.")
            # The first entry of the output represents a status code.
            elif output[0] == 0:
                logger.info("Processed file '{}' successfully.".format(output[1]))
                processed_files.append(output[2])
            elif output[0] == 1:
                logger.error("Error while processing '{}'".format(output[1]))
            elif output[0] == 2:
                logger.error(
                    "Processing output for '{}' did not match expected output.".format(
                        output[1]
                    )
                )
            else:
                raise ValueError("Unknown output format: '{}'.".format(output))

            remaining_files.remove(output[1])

            if delete_processed:
                logger.info("Deleting file '{}'.".format(output[1]))
                os.remove(output[1])

    logger.info("Finished handling requests. No remaining files or threads.")

    # After everything is done, terminate the processing process (by force
    # if it exceeds the time-out).
    logger.debug("Terminating AveragingWorker.")
    pipe_start.send("STOP_WORKER")
    processing_worker.join(2)
    processing_worker.terminate()
    processing_worker.join(2)
    processing_worker.close()
