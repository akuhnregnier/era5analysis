#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Determine the available datasets.

This information should match the documentation website at
https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation.


"""
import os

import yaml

__all__ = ["get_short_to_long", "get_table_dict", "load_era5_tables"]


def load_era5_tables():
    """Return tables of variable names.

    Returns:
        tables (dict): Dictionary containing the table information.
            tables['header_row'] contains the table header row as a list of strings,
            tables['rows'] contains a list of lists which make up the main table
                content, and
            tables['caption'] contains the table caption.
        common_table_header (iterable of strings): The unordered header columns common
            to all tables.

    """
    tables_dir = os.path.join(os.path.dirname(__file__), "tables")
    with open(os.path.join(tables_dir, "common_table_header.yaml"), "r") as f:
        common_table_header = yaml.safe_load(f)

    with open(os.path.join(tables_dir, "tables.yaml"), "r") as f:
        tables = yaml.safe_load(f)

    return tables, common_table_header


def get_short_to_long():
    """Get a mapping of short variable names to long variable names.

    The mapping is derived from the tables of variables found in the ERA5 data
    documentation.

    Returns:
        short_to_long (dict): Dictionary with short variable names as keys and long
            variable names as the corresponding values.

    """
    short_to_long = dict()
    tables, common_table_header = load_era5_tables()
    long_name_col = common_table_header.index("name")
    short_name_col = common_table_header.index("shortName")
    for data in tables.values():
        for row in data["rows"]:
            short_to_long[row[short_name_col]] = row[long_name_col]
    return short_to_long


def get_long_to_short():
    short_to_long = get_short_to_long()
    return dict((value, key) for (key, value) in short_to_long.items())


def get_table_dict():
    """Get a mapping of long variable names to all other properties.

    The mapping is derived from the tables of variables found in the ERA5 data
    documentation.

    Returns:
        table_dict (dict): Dictionary with long variable names as keys and a
            dictionary of properties as the corresponding values.

    """
    table_dict = dict()
    tables, common_table_header = load_era5_tables()
    long_name_col = common_table_header.index("name")
    for data in tables.values():
        for row in data["rows"]:
            long_name = row[long_name_col]
            table_dict[long_name] = {"caption": data["caption"]}
            for col, entry in enumerate(row):
                if col != long_name_col:
                    table_dict[long_name][common_table_header[col]] = entry

    return table_dict


if __name__ == "__main__":
    logging.config.dictConfig(LOGGING)
    tables, common_table_header = load_era5_tables()
    short_to_long = get_short_to_long()
    table_dict = get_table_dict()
