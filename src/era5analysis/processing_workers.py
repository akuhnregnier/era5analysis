#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import calendar
import multiprocessing
import os
import sys
import warnings
from abc import ABCMeta, abstractmethod
from datetime import datetime
from multiprocessing import Process

import iris
import iris.coord_categorisation
import numpy as np
from iris.util import monotonic

from .era5_tables import get_short_to_long
from .logging_config import logger

__all__ = ("Worker",)


short_to_long = get_short_to_long()


def assert_single_month(cube):
    """Assert that only a single month's data is present."""
    months = {
        cube.coord("time").cell(i).point.month
        for i in range(len(cube.coord("time").points))
    }
    years = {
        cube.coord("time").cell(i).point.year
        for i in range(len(cube.coord("time").points))
    }
    assert len(months) == len(years) == 1, (
        f"'{cube.name()}' cube should only contain data for one month in "
        f"one year, but got years '{years}' and months '{months}'."
    )


def ensure_datetime(datetime_obj):
    """If possible/needed, return a real datetime."""
    try:
        return datetime_obj._to_real_datetime()
    except AttributeError:
        return datetime_obj


def get_datetime_range(request_dict):
    """Get temporal extrema of the request."""
    years = list(map(int, request_dict["year"]))
    months = list(map(int, request_dict["month"]))
    days = list(map(int, request_dict["day"]))
    hours = [int(time.replace(":00", "")) for time in request_dict["time"]]

    # Since downloaded data can only reach the end of the last calendar month.
    max_days = min((max(days), calendar.monthrange(max(years), max(months))[1]))
    datetime_range = (
        datetime(min(years), min(months), min(days), min(hours)),
        datetime(max(years), max(months), max_days, max(hours)),
    )
    return datetime_range


class RegisterWorkers(ABCMeta):
    """Workers are registered into a central list to keep track of them.

    If any subclass (dataset) is used as another class's superclass, it is removed
    from the registry in favour of its subclasses.

    """

    workers = None

    def __init__(cls, name, bases, namespace):
        super().__init__(name, bases, namespace)
        if cls.workers is None:
            cls.workers = set()
        cls.workers.add(cls)
        cls.workers -= set(bases)

    def __iter__(cls):
        return iter(cls.workers)

    def __str__(cls):
        if cls in cls.workers:
            return cls.__name__
        return f"{cls.__name__}: {', '.join(map(str, cls))}"


class Worker(Process, metaclass=RegisterWorkers):
    """Abstract base class to subclass for use as a processing_worker in
    `retrieval_processing."""

    def __init__(self, id_index, pipe, *args, **kwargs):
        assert hasattr(self, "event"), "Must call assign_event."
        super().__init__(*args, **kwargs)
        self.id_index = id_index
        self.pipe = pipe

        # Configure a logger using `id_index`.
        self.logger = logger.bind(job_id=id_index)
        self.logger.debug(
            f"Initialised {self.__class__.__name__} with id_index={self.id_index}."
        )

    @abstractmethod
    def output_filename(self, input_filename):
        """Construct the output filename from the input filename."""

    @abstractmethod
    def process_cubes(cubes):
        """Process input cubes.

        Args:
            cubes (iris.cube.CubeList): List of cubes to process.

        Returns:
            iris.cube.CubeList: Processed cubes.

        """

    @classmethod
    def assign_event(cls, event):
        """Assign an Event to the class for shared usage.

        Args:
            queue (multiprocessing.synchronize.Event): Shared Event.

        """
        assert isinstance(event, multiprocessing.synchronize.Event)
        cls.event = event
        return cls

    @classmethod
    def check_output(cls, request, output=None):
        """Check that the output matches the request.

        Args:
            request (iterable of str, dict, str): A request tuple as returned
                by `retrieve`.
            output (None or iterable of int, str, str): Output of
                `Worker.process`. If None, this 3-element tuple will be
                recreated from the input request as expected in case of successful
                processing.

        Returns:
            bool: True if the output matches the request, False otherwise.

        """
        downloaded_file = request[2]
        if output is None:
            output = (0, downloaded_file, cls.output_filename(downloaded_file))
        logger.debug(f"Comparing request {request} and output {output}.")
        if output[0] != 0:
            logger.warning(
                "Output is not as expected because processing of the "
                f"request {request} failed with error code {output[0]}"
            )
            return False
        output_file = output[2]
        expected_file = cls.output_filename(downloaded_file)
        if output_file != expected_file:
            logger.warning(
                f"Filenames do not match. Expected '{expected_file}', "
                f"got '{output_file}'."
            )
            return False

        if not os.path.isfile(output_file):
            logger.warning(f"Expected output file '{output_file}' does not exist.")
            return False

        request_dict = request[1]
        datetime_range = get_datetime_range(request_dict)

        if any(hasattr(cls, custom_name) for custom_name in ("var_name", "long_name")):
            assert all(
                hasattr(cls, custom_name) for custom_name in ("var_name", "long_name")
            )
            if callable(cls.var_name):
                # Here, the name functions take the input variable names as their
                # argument, and return the names of the new cubes. The number of
                # output cubes (variables) is not pre-determined.
                assert callable(cls.long_name)
                # Short -> var_name, long -> long_name.
                expected_name_sets = []
                for variable in request_dict["variable"]:
                    var_names = cls.var_name(variable)
                    long_names = cls.long_name(short_to_long[variable])
                    for var_name, long_name in zip(var_names, long_names):
                        expected_name_sets.append({var_name, long_name})
                logger.info(
                    "Overriding requested names with "
                    f"{' and '.join([str(s) for s in expected_name_sets])}."
                )
            else:
                # In this case, the output will be one cube (with one set of names).
                expected_name_sets = [
                    {
                        getattr(cls, custom_name, None)
                        for custom_name in ("var_name", "long_name")
                    }
                ]
                logger.info(
                    "Overriding requested names with "
                    f"{' and '.join(expected_name_sets[0])}."
                )
        else:
            expected_name_sets = [
                {variable, short_to_long[variable]}
                for variable in request_dict["variable"]
            ]

        try:
            output_cubes = iris.load(output_file)
        except Exception:
            logger.exception("Error while loading '{}'.".format(output_file))
            return False

        # Check the temporal bounds for each cube and that all expected variables are
        # present exactly once overall, with each cube containing one variable.
        for cube in output_cubes:

            which_failed = []
            error_details = []

            if hasattr(cls, "cube_check_functions"):
                for cube_check_function in cls.cube_check_functions:
                    result = getattr(cls, cube_check_function)(cube, request)
                    if result is not None:
                        which_failed.append(result[0])
                        error_details.append(result[1])

            if (
                cube.coords("time")
                and len(cube.coord("time").points) > 1
                and not monotonic(cube.coord("time").points)
            ):
                which_failed.append("monotonic time check")
                error_details.append(
                    "Cube time coordinate did not increase monotonically."
                )
            if cube.coord("time").bounds is not None:
                cube_bounds = (
                    cube.coord("time").cell(0).bound[0],
                    cube.coord("time").cell(-1).bound[-1],
                )
            else:
                cube_bounds = tuple(
                    map(
                        ensure_datetime,
                        (
                            min(
                                cube.coord("time").cell(i).point
                                for i in range(len(cube.coord("time").points))
                            ),
                            max(
                                cube.coord("time").cell(i).point
                                for i in range(len(cube.coord("time").points))
                            ),
                        ),
                    )
                )

            if cube_bounds != datetime_range:
                which_failed.append("time bounds check")
                error_details.append(
                    f"Expected bounds '{datetime_range}', got bounds '{bounds}'."
                )

            raw_cube_names = (cube.standard_name, cube.long_name, cube.var_name)
            cube_names = list(map(str, raw_cube_names))

            for name_index, expected_name_set in enumerate(expected_name_sets):
                if expected_name_set.intersection(cube_names):
                    logger.debug(f"Matched '{expected_name_set}' with '{cube_names}'.")
                    # Next time, there will be one fewer variable to compare
                    # against.
                    del expected_name_sets[name_index]
                    break
            else:
                which_failed.append("variable name check")
                error_details.append(
                    "None of '{}' matched one of the expected names '{}.".format(
                        ", ".join(cube_names),
                        ", ".join(
                            [
                                str(name)
                                for name_set in expected_name_sets
                                for name in name_set
                            ]
                        ),
                    )
                )

            if which_failed:
                which_failed = " and ".join(which_failed)
                error_details = " ".join(error_details)
                logger.warning(
                    f"Failed '{which_failed}' for cube '{repr(cube)}'. "
                    f"'{error_details}'."
                )
                return False

        if expected_name_sets:
            logger.warning(
                f"Failed to find a variable name match for {expected_name_sets}."
            )
            return False
        return True

    def process(self, request):
        """Process data using `process_cubes`.

        Args:
            request (tuple): Request tuple as returned by `retrieve_monthly`.

        Returns:
            int: 0 for successful computation. 1 is returned if an error is
                encountered. Note that exceptions are merely logged and not raised.
            str: The original filename `filename`.
            str or None: The output filename (if successful) or None.

        """
        filename = request[2]
        self.logger.debug("Processing: '{}'.".format(filename))
        try:
            cubes = self.process_cubes(iris.load(filename))

            self.logger.debug("Finished processing cubes. Realising any lazy data now.")
            cubes.realise_data()

            self.logger.debug("Finished realising cubes. Saving now.")

            save_name = self.output_filename(filename)
            if not os.path.isdir(os.path.dirname(save_name)):
                os.makedirs(os.path.dirname(save_name))

            iris.save(cubes, save_name, zlib=False)

            # If everything went well.
            if self.check_output(request, (0, filename, save_name)):
                return (0, filename, save_name)

            return (2, filename, None)
        except Exception:
            self.logger.exception("Error while processing '{}'.".format(filename))
            return (1, filename, None)

    def run(self):
        try:
            self.logger.debug("Started listening for filenames to process.")
            while True:
                request = self.pipe.recv()
                if request == "STOP_WORKER":
                    self.logger.debug("STOP_WORKER received, breaking out of loop.")
                    break
                file_to_process = request[2]
                self.logger.debug(
                    "Received file: '{}'. Starting processing.".format(file_to_process)
                )
                output = self.process(request)
                self.logger.debug(
                    "Finished processing '{}' with status '{}'.".format(
                        file_to_process, output
                    )
                )
                self.pipe.send(output)
                self.logger.debug("Sent status '{}'.".format(output))
                self.logger.debug("Setting event flag.")
                self.event.set()
        except Exception:
            self.pipe.send(sys.exc_info())
        finally:
            self.logger.debug("Exiting.")


class NullWorker(Worker):
    """Change nothing about the filenames passed in via a pipe."""

    @staticmethod
    def output_filename(input_filename):
        """Construct the output filename from the input filename."""
        return input_filename.split(".nc")[0] + "_copy.nc"

    @staticmethod
    def process_cubes(cubes):
        """Nothing is done to the cubes here.

        Args:
            cubes (iris.cube.CubeList): List of cubes to process.

        Returns:
            iris.cube.CubeList: Processed cubes.

        """
        return cubes


class AveragingWorker(Worker):
    """Compute monthly averages using filenames passed in via a pipe."""

    @staticmethod
    def output_filename(input_filename):
        """Construct the output filename from the input filename."""
        return input_filename.split(".nc")[0] + "_monthly_mean.nc"

    @staticmethod
    def process_cubes(cubes):
        """Performs monthly averaging on the input cubes.

        Args:
            cubes (iris.cube.CubeList): List of cubes to process.

        Returns:
            iris.cube.CubeList: Processed cubes.

        """
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=(
                    "Collapsing a non-contiguous coordinate. Metadata may not "
                    "be fully descriptive for 'time'."
                ),
            )
            return iris.cube.CubeList(
                [cube.collapsed("time", iris.analysis.MEAN) for cube in cubes]
            )


class DailyAveragingWorker(Worker):
    """Compute daily averages using filenames passed in via a pipe."""

    # Additional functions run on every cube to verify data integrity.
    cube_check_functions = ["check_day_number"]

    @staticmethod
    def output_filename(input_filename):
        """Construct the output filename from the input filename."""
        return input_filename.split(".nc")[0] + "_daily_mean.nc"

    @staticmethod
    def check_day_number(cube, request):
        datetime_range = get_datetime_range(request[1])
        n_days = 1 + datetime_range[1].day - datetime_range[0].day
        cube_n_days = len(cube.coord("time").points)
        if cube_n_days != n_days:
            return (
                "Number of days check",
                f"Expected {n_days} days, but cube has {cube_n_days} days.",
            )
        return None

    @staticmethod
    def process_cubes(cubes):
        """Compute the daily averages of the input cubes.

        Args:
            cubes (iris.cube.CubeList): List of cubes to process.

        Returns:
            iris.cube.CubeList: Processed cubes.

        """
        output_cubes = iris.cube.CubeList()
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=(
                    "Collapsing a non-contiguous coordinate. Metadata may not "
                    "be fully descriptive for 'time'."
                ),
            )
            for cube in cubes:
                iris.coord_categorisation.add_day_of_year(cube, "time")
                iris.coord_categorisation.add_year(cube, "time")
                output_cubes.append(
                    cube.aggregated_by(["day_of_year", "year"], iris.analysis.MEAN)
                )
        return output_cubes


class CAPEPrecipWorker(Worker):
    """Compute monthly averages of hourly products of CAPE and Precipitation."""

    var_name = "CAPExP"
    long_name = "Product of CAPE and Precipitation"

    @staticmethod
    def output_filename(input_filepath):
        """Construct the output filename from the input filename."""
        input_dir, input_filename = os.path.split(input_filepath)
        return os.path.join(
            input_dir,
            "CAPE_P",
            input_filename.split(".nc")[0] + "_monthly_mean_cape_p.nc",
        )

    def process_cubes(self, cubes):
        """Compute CAPE x P from the input cubes.

        Args:
            cubes (iris.cube.CubeList): List of cubes to process.

        Returns:
            iris.cube.CubeList: Processed cubes.

        """
        assert len(cubes) == 2, "Expecting 2 cubes: CAPE and Precipitation."

        for cube in cubes:
            assert_single_month(cube)

        # Check that CAPE and Precipitation are both present.
        precip_names = {"Total precipitation", "tp"}
        cape_names = {"Convective available potential energy", "cape"}
        possible_names = [precip_names, cape_names]

        for cube in cubes:
            raw_cube_names = (cube.standard_name, cube.long_name, cube.var_name)
            cube_names = list(map(str, raw_cube_names))

            for name_index, name_set in enumerate(possible_names):
                if name_set.intersection(cube_names):
                    logger.debug(
                        "Matched '{}' with '{}'".format(possible_names, cube_names)
                    )
                    # Next time, there will be one fewer variable to compare
                    # against.
                    del possible_names[name_index]
                    break
            else:
                logger.error(
                    "None of '{}' matched one of the expected names '{}'.".format(
                        ", ".join(cube_names),
                        ", ".join(
                            [name for name_set in possible_names for name in name_set]
                        ),
                    )
                )

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=(
                    "Collapsing a non-contiguous coordinate. Metadata may not "
                    "be fully descriptive for 'time'."
                ),
            )

            # Compute the monthly average of the product of CAPE and
            # Precipitation.

            product_cube = (cubes[0] * cubes[1]).collapsed("time", iris.analysis.MEAN)

        product_cube.var_name = self.var_name
        product_cube.long_name = self.long_name

        return iris.cube.CubeList([product_cube])


class MonthlyMeanDailyMaxWorker(Worker):
    """Compute monthly means of daily maxima.

    Filenames to process are passed in via a pipe.

    """

    @staticmethod
    def output_filename(input_filename):
        """Construct the output filename from the input filename."""
        return input_filename.split(".nc")[0] + "_dailymax.nc"

    @staticmethod
    def process_cubes(cubes):
        """Compute the monthly mean of daily maxima of the input cubes.

        Args:
            cubes (iris.cube.CubeList): List of cubes to process.

        Returns:
            iris.cube.CubeList: Processed cubes.

        """
        new_cubes = []
        for cube in cubes:
            assert_single_month(cube)

            maxima_arrs = []
            data = cube.data
            assert (
                data.shape[0] % 24 == 0
            ), "There have to be an integer multiple of 24 hours!"
            for i in range(data.shape[0] // 24):
                day_slice = data[i * 24 : (i + 1) * 24]
                maxima = np.max(day_slice, axis=0)
                maxima_arrs.append(maxima[np.newaxis])

            combined_maxima = np.vstack((maxima_arrs))
            mean_max_temps = np.mean(combined_maxima, axis=0)

            time_coord = cube.coord("time").copy([np.mean(cube.coord("time").points)])
            if cube.coord("time").bounds is not None:
                time_coord.bounds = np.array(
                    [
                        [
                            cube.coord("time").bounds[0, 0],
                            cube.coord("time").bounds[-1, -1],
                        ]
                    ]
                )
            else:
                time_coord.bounds = np.array(
                    [[cube.coord("time").points.min(), cube.coord("time").points.max()]]
                )

            new_cube = iris.cube.Cube(
                mean_max_temps,
                dim_coords_and_dims=[
                    (cube.coord("latitude"), 0),
                    (cube.coord("longitude"), 1),
                ],
                aux_coords_and_dims=[(time_coord, None)],
                units=cube.units,
                var_name=cube.var_name,
                long_name=cube.long_name,
                attributes=cube.attributes,
            )
            new_cubes.append(new_cube)
        return new_cubes


class MonthlyMeanMinMaxWorker(Worker):
    """Compute the means, minima, and maxima for each month.

    Filenames to process are passed in via a pipe.

    """

    @staticmethod
    def var_name(name):
        """Get the new var names from the old name."""
        prefixes = ("mean_", "min_", "max_")
        if name is not None:
            return [prefix + name for prefix in prefixes]
        else:
            return [None] * len(prefixes)

    @staticmethod
    def long_name(name):
        """Get the new long names from the old name."""
        prefixes = ("Mean ", "Min ", "Max ")
        if name is not None:
            return [prefix + name for prefix in prefixes]
        else:
            return [None] * len(prefixes)

    @staticmethod
    def output_filename(input_filename):
        """Construct the output filename from the input filename."""
        return input_filename.split(".nc")[0] + "_monthly_mean_min_max.nc"

    @classmethod
    def process_cubes(cls, cubes):
        """Compute the monthly mean of daily maxima of the input cubes.

        Args:
            cubes (iris.cube.CubeList): List of cubes to process.

        Returns:
            iris.cube.CubeList: Processed cubes.

        """
        new_cubes = iris.cube.CubeList([])
        for cube in cubes:
            assert_single_month(cube)

            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message=(
                        "Collapsing a non-contiguous coordinate. Metadata may not "
                        "be fully descriptive for 'time'."
                    ),
                )
                mean_cube = cube.collapsed("time", iris.analysis.MEAN)
                min_cube = cube.collapsed("time", iris.analysis.MIN)
                max_cube = cube.collapsed("time", iris.analysis.MAX)

            mean_cube.long_name, min_cube.long_name, max_cube.long_name = cls.long_name(
                cube.long_name
            )
            mean_cube.var_name, min_cube.var_name, max_cube.var_name = cls.var_name(
                cube.var_name
            )

            new_cubes.extend([mean_cube, min_cube, max_cube])
        return new_cubes


class MonthlyMeanMinMaxDTRWorker(Worker):
    """Compute the means, minima, and maxima for each month.

    Filenames to process are passed in via a pipe.

    """

    @staticmethod
    def var_name(name):
        """Get the new var names from the old name."""
        prefixes = ("mean_", "min_", "max_", "dtr_")
        if name is not None:
            return [prefix + name for prefix in prefixes]
        else:
            return [None] * len(prefixes)

    @staticmethod
    def long_name(name):
        """Get the new long names from the old name."""
        prefixes = ("Mean ", "Min ", "Max ", "DTR ")
        if name is not None:
            return [prefix + name for prefix in prefixes]
        else:
            return [None] * len(prefixes)

    @staticmethod
    def output_filename(input_filename):
        """Construct the output filename from the input filename."""
        return input_filename.split(".nc")[0] + "_monthly_mean_min_max_dtr.nc"

    @classmethod
    def process_cubes(cls, cubes):
        """Compute the monthly mean of daily properties of the input cubes.

        Args:
            cubes (iris.cube.CubeList): List of cubes to process.

        Returns:
            iris.cube.CubeList: Processed cubes.

        """
        new_cubes = iris.cube.CubeList([])
        for cube in cubes:
            assert_single_month(cube)

            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message=(
                        "Collapsing a non-contiguous coordinate. Metadata may not "
                        "be fully descriptive for 'time'."
                    ),
                )
                mean_cube = cube.collapsed("time", iris.analysis.MEAN)
                min_cube = cube.collapsed("time", iris.analysis.MIN)
                max_cube = cube.collapsed("time", iris.analysis.MAX)

                iris.coord_categorisation.add_year(cube, "time")
                iris.coord_categorisation.add_day_of_year(cube, "time")

                daily_maxima = cube.aggregated_by(
                    ("day_of_year", "year"), iris.analysis.MAX
                )
                daily_minima = cube.aggregated_by(
                    ("day_of_year", "year"), iris.analysis.MIN
                )
                dtr_cube = (daily_maxima - daily_minima).collapsed(
                    "time", iris.analysis.MEAN
                )

            (
                mean_cube.long_name,
                min_cube.long_name,
                max_cube.long_name,
                dtr_cube.long_name,
            ) = cls.long_name(cube.long_name)
            (
                mean_cube.var_name,
                min_cube.var_name,
                max_cube.var_name,
                dtr_cube.var_name,
            ) = cls.var_name(cube.var_name)

            new_cubes.extend([mean_cube, min_cube, max_cube, dtr_cube])
        return new_cubes


# Automatically export all Worker subclass leaves defining individual datasets.
__all__ = list(set(__all__).union(set(map(str, Worker))))
