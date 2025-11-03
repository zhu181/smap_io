"""
Tests for the download module of GLDAS.
"""

import os
from argparse import Namespace
from datetime import datetime
from unittest.mock import MagicMock, patch

from smap_io.download_earthaccess import (dates_empty_folders,
                                          folder_get_first_last,
                                          get_first_folder,
                                          get_first_formatted_dir_in_dir,
                                          get_last_folder,
                                          get_last_formatted_dir_in_dir,
                                          get_start_date, main, parse_args)


def test_get_last_dir_in_dir():
    path = os.path.join(os.path.dirname(__file__), "smap_io-test-data", "SPL3SMP.006")
    last_dir = get_last_formatted_dir_in_dir(path, "{:%Y.%m.%d}")
    assert last_dir == "2020.04.02"


def test_get_first_dir_in_dir():
    path = os.path.join(os.path.dirname(__file__), "smap_io-test-data", "SPL3SMP.006")
    last_dir = get_first_formatted_dir_in_dir(path, "{:%Y.%m.%d}")
    assert last_dir == "2020.04.01"


def test_get_last_folder():
    path = os.path.join(os.path.dirname(__file__), "smap_io-test-data", "SPL3SMP.006")
    last = get_last_folder(path, ["{:%Y.%m.%d}"])
    last_should = os.path.join(path, "2020.04.02")
    assert last == last_should


def test_get_first_folder():
    path = os.path.join(os.path.dirname(__file__), "smap_io-test-data", "SPL3SMP.006")
    last = get_first_folder(path, ["{:%Y.%m.%d}"])
    last_should = os.path.join(path, "2020.04.01")
    assert last == last_should


def test_get_start_end():
    path = os.path.join(os.path.dirname(__file__), "smap_io-test-data", "SPL3SMP.006")
    start, end = folder_get_first_last(path)
    start_should = datetime(2020, 4, 1)
    end_should = datetime(2020, 4, 2)
    assert end == end_should
    assert start == start_should


def test_check_downloaded_data():
    path = os.path.join(os.path.dirname(__file__), "smap_io-test-data", "SPL3SMP.006")
    missing = dates_empty_folders(path)
    assert len(missing) == 0


def test_get_start_date_valid_product():
    # Test with a valid product that starts with "SPL3SMP"
    product = "SPL3SMP.001"
    expected_date = datetime(2015, 3, 31, 0)

    assert get_start_date(product) == expected_date


def test_get_start_date_invalid_product():
    # Test with a product that does not start with "SPL3SMP"
    product = "INVALID_PRODUCT"

    assert get_start_date(product) is None


def test_get_start_date_empty_string():
    # Test with an empty string
    product = ""

    assert get_start_date(product) is None


def test_get_start_date_partial_match():
    # Test with a string that includes "SPL3SMP" but does not start with it
    product = "123SPL3SMP"

    assert get_start_date(product) is None


@patch("smap_io.download.get_start_date")
@patch("smap_io.download.folder_get_first_last")
def test_parse_args_with_all_arguments(mock_folder_get_first_last, mock_get_start_date):
    mock_folder_get_first_last.return_value = (None, None)
    mock_get_start_date.return_value = datetime(2015, 3, 31)

    # Mock input arguments
    args = [
        "data",  # localroot
        "--start",
        "2023-01-01",
        "--end",
        "2023-01-31",
        "--product",
        "SPL4SMAU.004",
        "--filetypes",
        "h5",
        "nc",
        "txt",
        "--username",
        "user",
        "--password",
        "pass",
        "--n_proc",
        "4",
    ]

    # Call the function
    parsed_args = parse_args(args)

    # Expected result
    expected = Namespace(
        localroot="data",
        start=datetime(2023, 1, 1),
        end=datetime(2023, 1, 31),
        product="SPL4SMAU.004",
        filetypes=["h5", "nc", "txt"],
        username="user",
        password="pass",
        n_proc=4,
        urlroot="https://n5eil01u.ecs.nsidc.org",
        urlsubdirs=["SMAP", "SPL4SMAU.004", "%Y.%m.%d"],
        localsubdirs=["%Y.%m.%d"],
    )

    assert parsed_args == expected


@patch("smap_io.download.get_start_date")
@patch("smap_io.download.folder_get_first_last")
def test_parse_args_with_minimum_arguments(
    mock_folder_get_first_last, mock_get_start_date
):
    # Mock folder_get_first_last: no files found
    mock_folder_get_first_last.return_value = (None, None)
    mock_get_start_date.return_value = datetime(2015, 3, 31)

    # Mock input arguments
    args = ["data"]

    # Call the function
    parsed_args = parse_args(args)

    # Expected result
    expected = Namespace(
        localroot="data",
        start=mock_get_start_date.return_value,
        end=datetime.now(),
        product="SPL3SMP.008",  # Default product
        filetypes=["h5", "nc"],  # Default filetypes
        username=None,
        password=None,
        n_proc=1,  # Default processes
        urlroot="https://n5eil01u.ecs.nsidc.org",
        urlsubdirs=["SMAP", "SPL3SMP.008", "%Y.%m.%d"],
        localsubdirs=["%Y.%m.%d"],
    )

    assert parsed_args.localroot == expected.localroot
    assert parsed_args.start == expected.start
    assert parsed_args.product == expected.product
    assert parsed_args.filetypes == expected.filetypes
    assert parsed_args.username == expected.username
    assert parsed_args.password == expected.password
    assert parsed_args.n_proc == expected.n_proc
    assert parsed_args.urlroot == expected.urlroot
    assert parsed_args.urlsubdirs == expected.urlsubdirs
    assert parsed_args.localsubdirs == expected.localsubdirs


@patch("smap_io.download.get_start_date")
@patch("smap_io.download.folder_get_first_last")
def test_parse_args_with_folder_dates(mock_folder_get_first_last, mock_get_start_date):
    # Mock folder_get_first_last: last data date available
    mock_folder_get_first_last.return_value = (
        datetime(2022, 12, 25),
        datetime(2022, 12, 31),
    )
    mock_get_start_date.return_value = datetime(2015, 3, 31)

    # Mock input arguments
    args = ["data"]

    # Call the function
    parsed_args = parse_args(args)

    # Expected results:
    # - start defaults to the last date in the folder
    # - end defaults to now()
    expected_start = datetime(2022, 12, 31)
    expected_end = datetime.now()

    assert parsed_args.start == expected_start
    assert (
        isinstance(parsed_args.end, datetime)
        and parsed_args.end.date() == expected_end.date()
    )


@patch("smap_io.download.get_start_date")
@patch("smap_io.download.folder_get_first_last")
def test_parse_args_without_end_date(mock_folder_get_first_last, mock_get_start_date):
    mock_folder_get_first_last.return_value = (None, None)
    mock_get_start_date.return_value = datetime(2015, 3, 31)

    # Mock input arguments without an end date
    args = [
        "data",
        "--start",
        "2022-12-01",
    ]

    # Call the function
    parsed_args = parse_args(args)

    # Expect the default end date to be `datetime.now()`
    expected = datetime.now()

    assert isinstance(parsed_args.end, datetime)
    assert parsed_args.end.date() == expected.date()


@patch("smap_io.download.get_start_date")
@patch("smap_io.download.folder_get_first_last")
def test_parse_args_without_start_date(mock_folder_get_first_last, mock_get_start_date):
    # Mock folder_get_first_last to provide existing folder data
    mock_folder_get_first_last.return_value = (
        datetime(2022, 12, 25),
        datetime(2022, 12, 31),
    )
    mock_get_start_date.return_value = datetime(2015, 3, 31)

    # Mock input arguments without a start date
    args = [
        "data",
        "--end",
        "2023-01-01",
    ]

    # Call the function
    parsed_args = parse_args(args)

    # Expect the default start date to be taken from folder_get_first_last
    expected_start = datetime(2022, 12, 31)

    assert parsed_args.start == expected_start
    assert parsed_args.end == datetime(2023, 1, 1)


@patch("smap_io.download.dates_empty_folders")
@patch("smap_io.download.download_by_dt")
@patch("smap_io.download.daily")
@patch("smap_io.download.parse_args")
def test_main_retries_three_times_and_aborts(
    mock_parse_args, mock_daily, mock_download_by_dt, mock_dates_empty_folders
):
    # Mock `parse_args`
    mock_args = MagicMock()
    mock_args.start = datetime(2023, 1, 1)
    mock_args.end = datetime(2023, 1, 2)
    mock_args.localroot = "/path/to/local"
    mock_parse_args.return_value = mock_args

    # Mock `daily` to return 2 dates
    mock_daily.return_value = iter(
        [
            datetime(2023, 1, 1),
            datetime(2023, 1, 2),
        ]
    )

    # Mock `dates_empty_folders` to always return missing dates (to simulate failure to download)
    mock_dates_empty_folders.side_effect = [
        [datetime(2023, 1, 1), datetime(2023, 1, 2)],  # First attempt
        [datetime(2023, 1, 1), datetime(2023, 1, 2)],  # Second attempt
        [datetime(2023, 1, 1), datetime(2023, 1, 2)],  # Third (final) attempt
    ]

    # Call the main function
    main([])

    # Assertions
    # Assert `download_by_dt` is called 3 times (for 3 retries)
    assert mock_download_by_dt.call_count == 3

    # Assert `dates_empty_folders` is called after each retry to check missing dates
    assert mock_dates_empty_folders.call_count == 3

    # Assert `daily` is called once to generate the date range
    mock_daily.assert_called_once_with(mock_args.start, mock_args.end)


@patch("smap_io.download.dates_empty_folders")
@patch("smap_io.download.download_by_dt")
@patch("smap_io.download.daily")
@patch("smap_io.download.parse_args")
def test_main_some_dates_missing_after_retries(
    mock_parse_args, mock_daily, mock_download_by_dt, mock_dates_empty_folders
):
    # Mock `parse_args`
    mock_args = MagicMock()
    mock_args.start = datetime(2023, 1, 1)
    mock_args.end = datetime(2023, 1, 3)
    mock_args.localroot = "/path/to/local"
    mock_parse_args.return_value = mock_args

    # Mock `daily` to return 3 dates
    mock_daily.return_value = iter(
        [
            datetime(2023, 1, 1),
            datetime(2023, 1, 2),
            datetime(2023, 1, 3),
        ]
    )

    # Mock `dates_empty_folders` to simulate some dates remain missing after retries
    mock_dates_empty_folders.side_effect = [
        [datetime(2023, 1, 1), datetime(2023, 1, 2)],  # First attempt
        [datetime(2023, 1, 2)],  # Second attempt
        [datetime(2023, 1, 2)],  # Third (final) attempt
    ]

    # Call the main function
    main([])

    # Assertions
    # Assert `download_by_dt` is called 3 times (for 3 retries)
    assert mock_download_by_dt.call_count == 3

    # Assert `dates_empty_folders` is called after each retry
    assert mock_dates_empty_folders.call_count == 3

    # Assert `daily` function is called once
    mock_daily.assert_called_once_with(mock_args.start, mock_args.end)


@patch("smap_io.download.dates_empty_folders")
@patch("smap_io.download.download_by_dt")
@patch("smap_io.download.parse_args")
def test_main_no_initial_dates_to_download(
    mock_parse_args, mock_download_by_dt, mock_dates_empty_folders
):
    # Mock `parse_args`
    mock_args = MagicMock()
    mock_args.start = datetime(2023, 1, 1)
    mock_args.end = datetime(2023, 1, 1)
    mock_args.localroot = "/path/to/local"
    mock_parse_args.return_value = mock_args

    # Mock `daily` to return no dates
    mock_dates_empty_folders.return_value = []

    # Call the main function
    main([])
