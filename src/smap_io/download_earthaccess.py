"""
Download SMAP via earthaccess.
"""

import argparse
import glob
import os
import sys
from datetime import datetime, timedelta
import time

import earthaccess
import trollsift.parser as parser
from earthaccess.exceptions import LoginAttemptFailure

def mkdate(datestring):
    """
    Create datetime object from date string.

    Parameters
    ----------
    datestring : str
        Date string in format 'YYYY-MM-DD' or 'YYYY-MM-DDTHH:MM'
    
    Returns
    -------
    datetime.datetime
        Corresponding datetime object
    """
    if len(datestring) == 10:
        return datetime.strptime(datestring, '%Y-%m-%d')
    if len(datestring) == 16:
        return datetime.strptime(datestring, '%Y-%m-%dT%H:%M')

def daily(start, end):
    """
    Iterate over list of daily datetime objects.

    Parameters
    ----------
    start: datetime.datetime
        first date yielded
    end: datetime.datetime
        last date yielded

    Yields
    ------
    dt: datetime.datetime
        datetime object between start and end in daily steps.
    """
    td = timedelta(days=1)
    dt = start
    yield dt
    while True:
        dt = dt + td
        if dt > end:
            break
        yield dt

def dates_empty_folders(img_dir, crid=None):
    """
    Checks the download directory for date with empty folders.

    Parameters
    ----------
    img_dir : str
        Directory to count files and folders in
    crid : int, optional (default:None)
        If crid is passed, check if any file in each dir contains the crid in
        the name, else check if there is any file at all.
    Returns
    -------
    miss_dates : list
        Dates where a folder exists but no file is inside
    """

    missing = []
    for root, subdirs, files in os.walk(img_dir):
        if len(subdirs) != 0:
            continue
        if crid:
            cont = [str(crid) in afile for afile in files]
            if not any(cont):
                missing.append(root)
        else:
            cont = True if len(files) > 0 else False
            if not cont:
                missing.append(root)

    miss_dates = [
        datetime.strptime(os.path.basename(os.path.normpath(miss_path)), "%Y.%m.%d")
        for miss_path in missing
    ]

    return sorted(miss_dates)


def folder_get_first_last(
    root,
    fmt="SMAP_L3_SM_P_{time:%Y%m%d}_R{orbit:05d}_{proc_number:03d}.h5",
    subpaths=None,
):
    """
    Get first and last product which exists under the root folder.

    Parameters
    ----------
    root: string
        Root folder on local filesystem
    fmt: string, optional
        formatting string
    subpaths: list, optional
        format of the subdirectories under root. If None, defaults to ["{:%Y.%m.%d}"].

    Returns
    -------
    start: datetime.datetime
        First found product datetime
    end: datetime.datetime
        Last found product datetime
    """
    if subpaths is None:
        subpaths = ["{:%Y.%m.%d}"]

    start = None
    end = None
    first_folder = get_first_folder(root, subpaths)
    last_folder = get_last_folder(root, subpaths)

    if first_folder is not None:
        files = sorted(glob.glob(os.path.join(first_folder, parser.globify(fmt))))
        data = parser.parse(fmt, os.path.split(files[0])[1])
        start = data["time"]

    if last_folder is not None:
        files = sorted(glob.glob(os.path.join(last_folder, parser.globify(fmt))))
        data = parser.parse(fmt, os.path.split(files[-1])[1])
        end = data["time"]

    return start, end


def get_last_folder(root, subpaths):
    directory = root
    for level, subpath in enumerate(subpaths):
        last_dir = get_last_formatted_dir_in_dir(directory, subpath)
        if last_dir is None:
            directory = None
            break
        directory = os.path.join(directory, last_dir)
    return directory


def get_first_folder(root, subpaths):
    directory = root
    for level, subpath in enumerate(subpaths):
        last_dir = get_first_formatted_dir_in_dir(directory, subpath)
        if last_dir is None:
            directory = None
            break
        directory = os.path.join(directory, last_dir)
    return directory


def get_last_formatted_dir_in_dir(folder, fmt):
    """
    Get the (alphabetically) last directory in a directory
    which can be formatted according to fmt.
    """
    last_elem = None
    root_elements = sorted(os.listdir(folder))
    for root_element in root_elements[::-1]:
        if os.path.isdir(os.path.join(folder, root_element)):
            if parser.validate(fmt, root_element):
                last_elem = root_element
                break
    return last_elem


def get_first_formatted_dir_in_dir(folder, fmt):
    """
    Get the (alphabetically) first directory in a directory
    which can be formatted according to fmt.
    """
    first_elem = None
    root_elements = sorted(os.listdir(folder))
    for root_element in root_elements:
        if os.path.isdir(os.path.join(folder, root_element)):
            if parser.validate(fmt, root_element):
                first_elem = root_element
                break
    return first_elem


def get_start_date(product):
    if product.startswith("SPL3SMP"):
        return datetime(2015, 3, 31, 0)


def parse_args(args):
    """
    Parse command line parameters for recursive download

    :param args: command line parameters as list of strings
    :return: command line parameters as :obj:`argparse.Namespace`
    """
    parser = argparse.ArgumentParser(
        description="Download SMAP data. Register at https://urs.earthdata.nasa.gov/ first."
    )
    parser.add_argument(
        "localroot", help="Root of local filesystem where the data is stored."
    )
    parser.add_argument(
        "-s",
        "--start",
        type=mkdate,
        help=(
            "Startdate. Either in format YYYY-MM-DD or YYYY-MM-DDTHH:MM."
            " If not given then the target folder is scanned for a start date."
            " If no data is found there then the first available date of the product is used."
        ),
    )
    parser.add_argument(
        "-e",
        "--end",
        type=mkdate,
        help=(
            "Enddate. Either in format YYYY-MM-DD or YYYY-MM-DDTHH:MM."
            " If not given then the current date is used."
        ),
    )
    parser.add_argument(
        "--product_short_name",
        type=str,
        default="SPL3SMP",
        help="Short name of the SMAP product to download. (default: SPL3SMP)."
        " See also https://n5eil01u.ecs.nsidc.org/SMAP/ ",
    )
    parser.add_argument(
        "--version",
        type=str,
        default="008",
        help="Version of the SMAP product to download. (default: 008).",
    )
    parser.add_argument("--username", help="Username to use for download.")
    parser.add_argument("--password", help="password to use for download.")
    parser.add_argument(
        "--n_threads",
        default=8,
        type=int,
        help="Number of threads to use for downloading.",
    )
    parser.add_argument(
        "--retries",
        default=3,
        type=int,
        help="Number of times to retry a failed download (default: 3).",
    )
    parser.add_argument(
        "--retry-wait",
        default=5.0,
        type=float,
        help="Initial wait time in seconds between retries; exponential backoff is applied (default: 5.0).",
    )
    args = parser.parse_args(args)
    # set defaults that can not be handled by argparse

    if args.start is None or args.end is None:
        first, last = folder_get_first_last(args.localroot)
        if args.start is None:
            if last is None:
                args.start = get_start_date(args.product)
            else:
                args.start = last
        if args.end is None:
            args.end = datetime.now()

    print(
        f"Downloading SMAP {args.product} data from {args.start.isoformat()} "
        f"to {args.end.isoformat()} into folder {args.localroot}."
    )

    return args


def download_with_retries(result, local_path, retries, retry_wait):
    for attempt in range(retries):
        try:
            earthaccess.download(result, local_path=local_path)
            success = True
            break
        except Exception as e:
            # if more attempts remain, wait with exponential backoff and retry
            if attempt < retries - 1:
                wait = retry_wait * (2**attempt)
                print(
                    f"Download of {result.get('title', 'unknown')} failed on attempt {attempt+1}/{retries}: {e}. Retrying in {wait} seconds..."
                )
                time.sleep(wait)
            else:
                print(
                    f"Download of {result.get('title', 'unknown')} failed after {retries} attempts: {e}"
                )

    return success


def main(args):
    args = parse_args(args)
    try:
        if args.username and args.password:
            os.environ["EARTHDATA_USERNAME"] = args.username
            os.environ["EARTHDATA_PASSWORD"] = args.password
        elif args.token:
            os.environ["EARTHDATA_TOKEN"] = args.token
        earthaccess.login()
    except LoginAttemptFailure as e:
        print(f"Login failed: {e}")
        return

    dts = list(daily(args.start, args.end))
    results = earthaccess.search_data(
        short_name=args.product_short_name,
        version=args.version,
        temporal=(args.start, args.end),
    )
    for dt in dts:
        os.makedirs(
            os.path.join(args.localroot, dt.strftime("%Y.%m.%d")), exist_ok=True
        )
    dts = dates_empty_folders(args.localroot)
    for result in results:
        dt = result["umm"]["TemporalExtent"]
        # prepare local path for this product
        local_path = os.path.join(args.localroot, dt.strftime("%Y.%m.%d"))
        success = False
        if dt not in dts:
            continue
        success = download_with_retries(
            result, local_path, args.retries, args.retry_wait
        )
        if not success:
            continue
    dts = dates_empty_folders(args.localroot)
    for dt in dts:
        print(f"No data downloaded for date {dt.date()}")

    if len(dts) != 0:
        print("----------------------------------------------------------")
        print("----------------------------------------------------------")
        print("No data has been downloaded for the following dates:")
        for date in dts:
            print(str(date.date()))


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
