import json
import os

from callsigns.fetcher import fetch_and_extract_all
from callsigns.parser import parse_all_raw
from callsigns.parser import records_by_call_sign
from callsigns.parser import to_license_records


def build(rootdir: str = '_build') -> None:
    fetch_and_extract_all()
    raw_records = parse_all_raw()
    license_records = to_license_records(raw_records)
    call_sign_records = records_by_call_sign(license_records)
    callsign_dir = os.path.join(rootdir, 'callsigns')
    if not os.path.exists(rootdir):
        os.mkdir(rootdir)
    if not os.path.exists(callsign_dir):
        os.mkdir(callsign_dir)
    for callsign, records in call_sign_records.items():
        fp = os.path.join(callsign_dir, f'{callsign}.json')
        formatted = [r.as_dict() for r in records]
        with open(fp, 'w') as f:
            json.dump(formatted, f)


if __name__ == '__main__':
    build()
