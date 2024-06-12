from __future__ import annotations

import copy
import datetime
import json
import os
import pathlib
import re
import tempfile
import time
from hashlib import md5
from typing import Generator

from callsigns.fetcher import fetch_and_extract_all
from callsigns.parser import parse_all_raw
from callsigns.parser import records_by_call_sign
from callsigns.parser import to_license_records
from callsigns.uploader import Uploader


def build(
    rootdir: str = '_build',
    flat: bool = True,
    dry_run: bool = False,
    quiet: int = 0,
    hash_file: str | None = None,
    remote_hashes: dict[str, str] | None = None,
) -> Generator[str, None, None]:
    if quiet < 2:
        print('fetching...')
    fetch_and_extract_all()
    if quiet < 2:
        print('parsing...')
    raw_records = parse_all_raw()
    if quiet < 2:
        print('converting records...')
    license_records = to_license_records(raw_records)
    if quiet < 2:
        print('sorting...')
    call_sign_records = records_by_call_sign(license_records)
    callsign_dir = os.path.join(rootdir, 'callsigns')
    if not os.path.exists(rootdir):
        os.mkdir(rootdir)
    if not os.path.exists(callsign_dir):
        os.mkdir(callsign_dir)
    if quiet < 2:
        print('processing...')
    num_records = len(call_sign_records)
    skipped = 0
    changed = 0
    to_sync = 0
    new = 0

    if remote_hashes is None:
        remote_hashes = {}
    if hash_file is not None:
        if os.path.isfile(hash_file):
            with open(hash_file, encoding='utf-8') as f:
                hash_data = json.load(f)
                local_record_hashes = hash_data['hashes']
        else:
            local_record_hashes = copy.copy(remote_hashes)
    else:
        hash_file = os.path.join(rootdir, 'hashes.json')
        local_record_hashes = copy.copy(remote_hashes)
    current_record_hashes = {}
    # to_upload = []
    for index, (callsign, records) in enumerate(call_sign_records.items(), start=1):
        if not quiet and (index % 100 == 0 or index == num_records):
            print(f'Processing {index}/{num_records} {skipped=} {changed=} {new=} {to_sync=}          ', end='\r')
        if not flat:
            pattern = r'([A-Z]+)(\d)[A-Z]+'
            match = re.match(pattern, callsign)
            if not match:
                print(f'could not parse callsign {callsign!r} {records!r}')
                continue
            call_prefix, region_num = match.groups()
            callsign_subdir = os.path.join(callsign_dir, region_num, call_prefix)
            if not os.path.exists(callsign_subdir):
                os.makedirs(callsign_subdir)
            fp = pathlib.Path(os.path.join(callsign_subdir, f'{callsign}.json')).as_posix()
        else:
            fp = pathlib.Path(os.path.join(callsign_dir, f'{callsign}.json')).as_posix()
        formatted = [r.as_dict() for r in records]
        out_bytes = json.dumps(formatted, separators=(',', ':')).encode('utf-8')
        out_digest = md5(out_bytes).hexdigest()
        current_record_hashes[fp] = out_digest
        if fp in local_record_hashes:
            existing_digest = local_record_hashes[fp]

            if existing_digest == out_digest:
                if remote_hashes.get(fp) != existing_digest:
                    # assert pathlib.Path(fp).relative_to(rootdir).as_posix() == fp
                    # to_upload.append(pathlib.Path(fp).relative_to(rootdir).as_posix())
                    yield pathlib.Path(fp).relative_to(rootdir).as_posix()
                    to_sync += 1
                    continue
                else:
                    skipped += 1
                    continue
        if os.path.exists(fp):
            exists = True
            with open(fp, 'rb') as f:
                data = f.read()
            existing_digest = md5(data).hexdigest()
            if existing_digest == out_digest:
                if remote_hashes.get(fp) != existing_digest:
                    # to_upload.append(pathlib.Path(fp).relative_to(rootdir).as_posix())
                    yield pathlib.Path(fp).relative_to(rootdir).as_posix()
                    to_sync += 1
                    continue
                else:
                    skipped += 1
                    continue
        else:
            exists = False

        out_text = json.dumps(formatted, separators=(',', ':'))
        if not dry_run:
            with open(fp, 'wb') as f:
                f.write(out_text.encode('utf-8'))
        if exists:
            changed += 1
        else:
            new += 1
        #         to_upload.append(pathlib.Path(fp).relative_to(rootdir).as_posix())
        yield pathlib.Path(fp).relative_to(rootdir).as_posix()

    if not dry_run:
        # todo: make this atomic
        hashdata = {'created_at': time.time(), 'hashes': current_record_hashes}
        with open(hash_file, 'w', encoding='utf-8') as hfile:
            json.dump(hashdata, hfile, separators=(',', ':'))
    if quiet < 2:
        print(f'{num_records} records processed. {skipped=} {changed=} {new=}')

    # return to_upload, hash_file


def _get_remote_hashes(bucket: str, key: str = 'hashes.json') -> dict[str, str] | None:
    # TODO: [de]compress remote hash file
    import boto3

    client = boto3.client('s3')
    with tempfile.TemporaryDirectory(prefix='callsigns-temp', ignore_cleanup_errors=True) as d:
        tempfilename = f'{d}/remote-hashes.json'
        try:
            client.download_file(bucket, key, tempfilename)
        except Exception as e:
            print(e)
            return None
        with open(tempfilename, 'r', encoding='utf-8') as f:
            remote_hash_data = json.load(f)['hashes']
            assert isinstance(remote_hash_data, dict)
        return remote_hash_data


def _update_hashes_to_remote(local_hash_file: str, bucket: str, key: str = 'hashes.json') -> None:
    import boto3

    client = boto3.client('s3')
    try:
        client.upload_file(local_hash_file, bucket, key)
        print('updated remote')
    except Exception as e:
        print(e)


def _upload_error_logs(bucket: str, errors: list[tuple[str, str]]) -> None:
    import boto3

    s3 = boto3.resource('s3')
    now = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')
    key = f'errors/{now}.errors.json'
    contents = json.dumps(errors).encode('utf-8')
    obj = s3.Object(bucket, key)
    obj.put(Body=contents)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--rootdir', type=str, default='_build')
    parser.add_argument('--no-flat', action='store_false', dest='flat', default=True)
    parser.add_argument('--dry-run', action='store_true', dest='dry_run', default=False)
    parser.add_argument('-q', '--quiet', action='count', dest='quiet', default=0)
    parser.add_argument('--upload-bucket', dest='bucket')
    args = parser.parse_args()
    if os.environ.get('CI'):
        quiet = 1
    else:
        quiet = args.quiet

    hashfile = os.path.join(args.rootdir, 'hashes.json')
    if args.bucket and not args.dry_run:
        remote_hashes = _get_remote_hashes(args.bucket)
    else:
        remote_hashes = None
    import logging

    logging.basicConfig(level=logging.WARN)
    if args.bucket:
        uploader = Uploader(rootdir=args.rootdir, bucket_name=args.bucket, _dry_run=args.dry_run)
    else:
        uploader = None
    try:
        for key in build(
            rootdir=args.rootdir,
            flat=args.flat,
            dry_run=args.dry_run,
            hash_file=hashfile,
            quiet=quiet,
            remote_hashes=remote_hashes,
        ):
            if uploader is not None:
                uploader.queue_upload(key)
    except Exception as e:
        print('fatal exception', e)
    print('Waiting for uploads to complete...')
    if uploader is not None:
        uploader.join()
        print(len(uploader.upload_errors), 'upload errors')
        if args.bucket and uploader.upload_errors:
            print('uploading error logs')
            _upload_error_logs(args.bucket, uploader.upload_errors)
    print('Updating remote hashes')
    if args.bucket and not args.dryrun:
        _update_hashes_to_remote(hashfile, args.bucket)
    print('Done')


if __name__ == '__main__':
    main()
