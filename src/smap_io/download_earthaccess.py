"""
Download SMAP via earthaccess.
"""

import os
import sys
import glob
import argparse
from functools import partial

import trollsift.parser as parser
from datetime import datetime
from datedown.interface import mkdate
from datedown.dates import daily
from datedown.urlcreator import create_dt_url
from datedown.fname_creator import create_dt_fpath
from datedown.interface import download_by_dt
import subprocess
import tempfile
from multiprocessing import Pool
import earthaccess


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
    for dir, subdirs, files in os.walk(img_dir):
        if len(subdirs) != 0:
            continue
        if crid:
            cont = [str(crid) in afile for afile in files]
            if not any(cont):
                missing.append(dir)
        else:
            cont = True if len(files) > 0 else False
            if not cont:
                missing.append(dir)

    miss_dates = [
        datetime.strptime(os.path.basename(os.path.normpath(miss_path)), "%Y.%m.%d")
        for miss_path in missing
    ]

    return sorted(miss_dates)

def folder_get_first_last(
        root,
        fmt="SMAP_L3_SM_P_{time:%Y%m%d}_R{orbit:05d}_{proc_number:03d}.h5",
        subpaths=['{:%Y.%m.%d}']):
    """
    Get first and last product which exists under the root folder.

    Parameters
    ----------
    root: string
        Root folder on local filesystem
    fmt: string, optional
        formatting string
    subpaths: list, optional
        format of the subdirectories under root.

    Returns
    -------
    start: datetime.datetime
        First found product datetime
    end: datetime.datetime
        Last found product datetime
    """
    start = None
    end = None
    first_folder = get_first_folder(root, subpaths)
    last_folder = get_last_folder(root, subpaths)

    if first_folder is not None:
        files = sorted(
            glob.glob(os.path.join(first_folder, parser.globify(fmt))))
        data = parser.parse(fmt, os.path.split(files[0])[1])
        start = data['time']

    if last_folder is not None:
        files = sorted(
            glob.glob(os.path.join(last_folder, parser.globify(fmt))))
        data = parser.parse(fmt, os.path.split(files[-1])[1])
        end = data['time']

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
        "localroot", help='Root of local filesystem where the data is stored.')
    parser.add_argument(
        "-s",
        "--start",
        type=mkdate,
        help=(
            "Startdate. Either in format YYYY-MM-DD or YYYY-MM-DDTHH:MM."
            " If not given then the target folder is scanned for a start date."
            " If no data is found there then the first available date of the product is used."
        ))
    parser.add_argument(
        "-e",
        "--end",
        type=mkdate,
        help=("Enddate. Either in format YYYY-MM-DD or YYYY-MM-DDTHH:MM."
              " If not given then the current date is used."))
    parser.add_argument(
        "--product",
        type=str,
        default="SPL3SMP.008",
        help='SMAP product to download. (default: SPL3SMP.008).'
        ' See also https://n5eil01u.ecs.nsidc.org/SMAP/ ')
    parser.add_argument(
        "--filetypes",
        nargs="*",
        default=["h5", "nc"],
        help="File types (extensions) to download. Files with"
        "other extensions are ignored. "
        "Default is equivalent to --filetypes h5 nc")
    parser.add_argument("--username", help='Username to use for download.')
    parser.add_argument("--password", help='password to use for download.')
    parser.add_argument(
        "--n_proc",
        default=1,
        type=int,
        help='Number of parallel processes to use for downloading.')
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

    args.urlroot = 'https://n5eil01u.ecs.nsidc.org'
    args.urlsubdirs = ['SMAP', args.product, '%Y.%m.%d']
    args.localsubdirs = ['%Y.%m.%d']

    print(
        f"Downloading SMAP {args.product} data from {args.start.isoformat()} "
        f"to {args.end.isoformat()} into folder {args.localroot}.")

    return args

def main(args):
    args = parse_args(args)

    dts = list(daily(args.start, args.end))
    i = 0
    while (len(dts) != 0) and i < 3:  # after 3 reties abort
        url_create_fn = partial(
            create_dt_url,
            root=args.urlroot,
            fname='',
            subdirs=args.urlsubdirs)
        fname_create_fn = partial(
            create_dt_fpath,
            root=args.localroot,
            fname='',
            subdirs=args.localsubdirs)
        down_func = partial(
            download,
            num_proc=args.n_proc,
            username=args.username,
            password=args.password,
            recursive=True,
            filetypes=args.filetypes,
            robots_off=True)

        download_by_dt(
            dts, url_create_fn, fname_create_fn, down_func, recursive=True)

        dts = dates_empty_folders(args.localroot)  # missing dates
        i += 1

    if len(dts) != 0:
        print('----------------------------------------------------------')
        print('----------------------------------------------------------')
        print('No data has been downloaded for the following dates:')
        for date in dts:
            print(str(date.date()))


def run():
    main(sys.argv[1:])

if __name__ == '__main__':
    run()
