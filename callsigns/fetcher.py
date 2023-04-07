from __future__ import annotations

import csv
import datetime
import os
import pathlib
import sys
import urllib.request
import zipfile

import dateutil.parser
import requests
from dateutil import tz

_days = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat']
DAILY_URL_PATTERN = 'https://data.fcc.gov/download/pub/uls/daily/l_am_{}.zip'
DAILY_URLS = [DAILY_URL_PATTERN.format(day) for day in _days]
WEEKLY_URL = 'https://data.fcc.gov/download/pub/uls/complete/l_amat.zip'
EASTERN = tz.gettz('US/Eastern')


FCC_HS_FIELD_NAMES = [
    'Record Type [HS]',
    'Unique System Identifier',
    'ULS File Number',
    'Call Sign',
    'Log Date',
    'Code',
]


def get_all_callsigns(*hs_files: str) -> set[str]:
    """
    Given a set of HS record files, returns a set of all unique callsigns contained in those files.
    """
    callsigns = set()
    for hs_file in hs_files:
        with open(hs_file, 'r') as f:
            reader = csv.DictReader(f, fieldnames=FCC_HS_FIELD_NAMES, delimiter='|')
            for row in reader:
                cs = row['Call Sign']
                callsigns.add(cs)
    return callsigns


def _parse_counts_date_header(head: str) -> datetime.datetime:
    if 'File Creation Date: ' not in head:
        raise ValueError('malformed counts file')
    _, datestring = head.split('File Creation Date: ')

    dt = dateutil.parser.parse(datestring.strip(), tzinfos={'EDT': EASTERN, 'EST': EASTERN})
    return dt


def _get_data_dir_date(data_dir: pathlib.Path) -> datetime.datetime:
    counts_file = data_dir / 'counts'
    with open(counts_file) as f:
        head = f.readline()
        # e.g. 'File Creation Date: Tue Mar 14 08:00:35 EDT 2023'
    return _parse_counts_date_header(head)


class DataDirExists(OSError):
    ...


def _zip_is_newer(zip_fp: pathlib.Path, data_dir: pathlib.Path) -> bool:
    if not os.path.exists(data_dir / 'counts'):
        return True
    existing_date = _get_data_dir_date(data_dir)
    with zipfile.ZipFile(zip_fp) as zip:
        with zip.open('counts') as f:
            head = str(f.readline(), 'utf-8')
    new_date = _parse_counts_date_header(head)
    if new_date > existing_date:
        return True
    else:
        return False


def _fetch_archive(archive_url: str, dest_dir: pathlib.Path, extract: bool = True) -> None:
    zip_fp = (dest_dir / 'archive.zip').absolute()
    if os.path.exists(zip_fp):
        r = requests.head(archive_url)
        r.raise_for_status()
        content_length = int(r.headers['Content-Length'])

        if os.stat(zip_fp).st_size != content_length:
            # XXX: technically, a different archive CAN have the same size, but I feel this is pretty unlikely.
            # For now, it's probably not worth worrying about (I hope)
            tmpfile = str(zip_fp) + '.tmp'
            print('Downloading {}'.format(archive_url), file=sys.stderr)
            urllib.request.urlretrieve(archive_url, tmpfile)
            print('Done', file=sys.stderr)
            os.remove(zip_fp)
            os.rename(tmpfile, zip_fp)
    else:
        print('Downloading {}'.format(archive_url), file=sys.stderr)
        urllib.request.urlretrieve(archive_url, zip_fp)
        print('Done', file=sys.stderr)
    if extract:
        if _zip_is_newer(zip_fp, dest_dir):
            print('Extracting {}'.format(zip_fp), file=sys.stderr)
            with zipfile.ZipFile(zip_fp) as zip:
                zip.extractall(dest_dir)


def _should_get_day(last_weekly_date: datetime.date, day: str) -> bool:
    url = DAILY_URL_PATTERN.format(day)
    r = requests.head(url)
    r.raise_for_status()
    last_modified_header = r.headers['Last-Modified']
    last_modified_date = dateutil.parser.parse(last_modified_header)
    if last_modified_date.date() > last_weekly_date:
        return True
    else:
        return False


def fetch_and_extract_all(data_dir: pathlib.Path | str = 'callsign_data', exists_ok: bool = True) -> list[str]:
    if os.path.exists(data_dir) and not exists_ok:
        raise DataDirExists(data_dir)
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)
    data_dir = pathlib.Path(data_dir)
    weekly_bin_dir = data_dir / 'weekly'

    if not os.path.exists(weekly_bin_dir):
        os.mkdir(weekly_bin_dir)

    _fetch_archive(WEEKLY_URL, weekly_bin_dir)

    dt = _get_data_dir_date(weekly_bin_dir)
    previous_week = dt - datetime.timedelta(days=7)
    assert (
        dt.weekday() == 6 and previous_week.weekday() == 6
    ), f'{dt} day != {previous_week} day ({dt.day!r} != {previous_week.day!r}'
    previous_sunday = previous_week.date()

    process_dirs = [str(weekly_bin_dir.absolute())]

    for day in _days:
        day_dir = data_dir / day
        if not os.path.exists(day_dir):
            os.mkdir(day_dir)
        if _should_get_day(previous_sunday, day):
            _fetch_archive(DAILY_URL_PATTERN.format(day), day_dir)
            process_dirs.append(str(day_dir.absolute()))

    return process_dirs


if __name__ == '__main__':
    fetch_and_extract_all()
