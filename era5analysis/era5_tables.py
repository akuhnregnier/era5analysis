#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Determining the available datasets by parsing the documentation website.

"""
import re
from functools import reduce

import requests
from bs4 import BeautifulSoup

from joblib import Memory

from .data import DATA_DIR
from .logging_config import logger

__all__ = ["get_short_to_long", "get_table_dict", "load_era5_tables"]


# The URL has moved.
# URL = "https://confluence.ecmwf.int/display/CKB/ERA5+data+documentation"
URL = "https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation"

memory = Memory(location=DATA_DIR, verbose=0)


@memory.cache
def load_era5_tables(url=URL):
    """Parse ERA5 data documentation website for tables of variable names.

    Note:
        Since this function is cached using joblib.Memory.cache, a new
        output will only be computed for a given url if this function has
        never been called with this url. To execute this function again,
        delete the cache directory './cachedir'.

    Args:
        url (str): The url of the data documentation.

    Returns:
        tables (dict): Dictionary containing the table information.
            tables['header_row'] contains the table header row as a list of
                strings,
            tables['rows'] contains a list of lists which
                make up the main table content, and
            tables['caption'] contains the table caption.
        common_table_header (iterable of strings): The unordered header columns common
            to all tables.

    """
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    # Only interested in tables 1 - 13.
    table_pattern = re.compile(r"(Table \b(?:[1-9]|1[0-3])):")

    def target_table(tag):
        # Test if tag if True first to avoid performing operations on it if
        # it is None (or '') which could throw an error.
        return (
            tag
            and "table-wrap" in tag.get("class", [None])
            and table_pattern.search(str(tag.previous_element))
        )

    def div_header_tags(tag):
        return (
            tag
            and tag.name == "div"
            and tag.has_attr("class")
            and "tablesorter-header-inner" in tag.get("class")
            and tag.parent.parent.parent.has_attr("class")
            and ("tableFloatingHeaderOriginal" in tag.parent.parent.parent.get("class"))
        )

    def th_header_tags(tag):
        return (
            tag
            and tag.name == "th"
            and tag.has_attr("class")
            and "confluenceTh" in tag.get("class")
        )

    # Depending on how the html source code is downloaded, the tags differ.
    header_funcs = (div_header_tags, th_header_tags)

    tables = {}
    found_table_tags = soup.find_all(target_table)
    logger.debug("Found {} table tags.".format(len(found_table_tags)))
    for table_tag in found_table_tags:
        search_result = table_pattern.search(table_tag.previous_element)
        assert search_result, (
            "target_table function should only discover tables matching "
            "table_pattern."
        )

        table_name = search_result.group(1)

        rows = []
        row_tags = table_tag.find_all("tr")
        logger.debug(
            "Found {} rows for table {} (might include header).".format(
                len(row_tags), table_name
            )
        )

        header_tags = max(
            [table_tag.find_all(header_func) for header_func in header_funcs], key=len
        )
        logger.debug(
            "Found {} header tags for table {}.".format(len(header_tags), table_name)
        )
        for row_tag in row_tags:
            entries = row_tag.find_all("td")

            row_contents = []
            for entry in entries:
                if entry.find_all("div", class_="content-wrapper"):
                    expand_spans = entries[1].find_all(
                        "span", class_="expand-control-text"
                    )
                    assert len(expand_spans) == 1
                    text = expand_spans[0].get_text().replace("\xa0", "")
                else:
                    text = entry.get_text().replace("\xa0", "")

                row_contents.append(text)

            if row_contents:
                rows.append(row_contents)

        if header_tags:
            header = [tag.get_text() for tag in header_tags]
        else:
            header = rows[0]
            rows = rows[1:]

        # Process rows
        try:
            for row_index in range(len(rows)):
                rows[row_index][0] = int(rows[row_index][0])
        except ValueError:
            logger.exception("First column should be an integer index!")
            raise

        if len(rows) > 1:
            assert all(
                len(row) == len(rows[0]) for row in rows[1:]
            ), "All rows must have the same length."

        tables[table_name] = {
            "header_row": header,
            "rows": rows,
            "caption": str(table_tag.previous_element),
        }

    headers = []
    for data in tables.values():
        headers.append(tuple(data["header_row"]))

    logger.info("Found {} tables.".format(len(tables)))
    common_table_header = reduce(lambda x, y: set(x).intersection(y), set(headers))

    essential_columns = {"count", "name", "units", "shortName", "paramId", "an", "fc"}
    assert essential_columns.issubset(
        common_table_header
    ), "Not all tables contained the essential columns."

    # Make sure that all tables only have the expected columns.
    # NOTE: Currently this removes the 'Variable name in CDS' in column, since not all
    # tables have this column!
    for data in tables.values():
        # If the header does not match, we need to trim columns.
        header = data["header_row"]
        header_set = set(header)
        if common_table_header != header_set:
            extraneous_columns = header_set - common_table_header
            indices_to_delete = [
                i for i, c in enumerate(header) if c in extraneous_columns
            ]

            for i in sorted(indices_to_delete, reverse=True):
                del header[i]
                for row in data["rows"]:
                    del row[i]

        assert len(data["header_row"]) == len(
            data["rows"][0]
        ), "The number of data columns must match the header."

    # The final returned table header needs to be ordered the same way as the table,
    # which is most likely not the case when using sets.
    return tables, tuple(name for name in headers[0] if name in common_table_header)


def get_short_to_long(url=URL):
    """Get a mapping of short variable names to long variable names.

    The mapping is derived from the tables of variables found at the given
    url, which should point to the ERA5 data documentation.

    Args:
        url (str): The url of the data documentation.

    Returns:
        short_to_long (dict): Dictionary with short variable names as keys
            and long variable names as the corresponding values.

    """
    short_to_long = dict()
    tables, common_table_header = load_era5_tables(url)
    long_name_col = common_table_header.index("name")
    short_name_col = common_table_header.index("shortName")
    for data in tables.values():
        for row in data["rows"]:
            short_to_long[row[short_name_col]] = row[long_name_col]
    return short_to_long


def get_table_dict(url=URL):
    """Get a mapping of long variable names to all other properties.

    The mapping is derived from the tables of variables found at the given
    url, which should point to the ERA5 data documentation.

    Args:
        url (str): The url of the data documentation.

    Returns:
        table_dict (dict): Dictionary with long variable names as keys
            and a dictionary of properties as the corresponding values.

    """
    table_dict = dict()
    tables, common_table_header = load_era5_tables(url)
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
