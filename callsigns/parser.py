from __future__ import annotations

import csv
import os
import pathlib
import re
import typing
from typing import Any

from .constants import FCC_AM_FIELD_NAMES
from .constants import FCC_EN_FIELD_NAMES
from .constants import FCC_HD_FIELD_NAMES
from .constants import MORSE_TABLE
from .constants import PHONETIC_WORDS
from .constants import SYLLABLE_LENGTHS
from .constants import LICENSE_STATUS_CODES
from .constants import OPERATOR_CLASS_CODES
from .fetcher import _get_data_dir_date


def parse_file(filename: str | pathlib.Path, field_names: list[str]) -> list[dict[str, Any]]:
    with open(filename) as f:
        reader = csv.DictReader(f, fieldnames=field_names, delimiter='|', quoting=csv.QUOTE_NONE)
        rows = list(reader)
    return rows


def parse_all_raw(data_root: str = 'callsign_data') -> dict[str, dict[str, dict[str, Any]]]:
    root = pathlib.Path(data_root)
    weekly = root / 'weekly'
    included = [weekly]
    weekly_date = _get_data_dir_date(weekly)
    for dirname in os.listdir(data_root):
        if 'weekly' in dirname:
            continue
        path = root / dirname
        dir_date = _get_data_dir_date(path)
        if dir_date >= weekly_date:
            included.append(path)
    included.sort(key=lambda d: _get_data_dir_date(d))

    records = {}
    record_fields = (FCC_HD_FIELD_NAMES, FCC_AM_FIELD_NAMES, FCC_EN_FIELD_NAMES)
    record_types = ('HD', 'AM', 'EN')
    for record_type, field_names in zip(record_types, record_fields):
        record_rows = []
        for path in included:
            record_file = path / f'{record_type}.dat'
            if not os.path.exists(record_file):
                continue  # sometimes, there are no records for a day (sundays, especially)
            rows = parse_file(record_file, field_names=field_names)
            record_rows.extend(rows)
        records[record_type] = record_rows
    records_by_usi: dict[str, dict[str, dict[str, Any]]] = {}
    for record_type, record_list in records.items():
        for record in record_list:
            usi: str = record['Unique System Identifier']
            if usi not in records_by_usi:
                records_by_usi[usi] = {record_type: record}
            else:
                records_by_usi[usi][record_type] = record
    return records_by_usi


def to_license_records(raw_records: dict[str, dict[str, dict[str, Any]]]) -> dict[str, LicenseRecord]:
    license_records: dict[str, LicenseRecord] = {}
    for usi, record_data in raw_records.items():
        call_sign = record_data['HD']['Call Sign']
        status = LICENSE_STATUS_CODES[record_data['HD']['License Status']]
        frn = record_data['EN']['FCC Registration Number (FRN)']
        first_name = record_data['EN']['First Name']
        middle_initial = record_data['EN']['MI']
        last_name = record_data['EN']['Last Name']
        street_address = record_data['EN']['Street Address']
        attn_line = record_data['EN']['Attention Line']
        city = record_data['EN']['City']
        state = record_data['EN']['State']
        zip_code = record_data['EN']['Zip Code']
        po_box = record_data['EN']['PO Box']
        grant_date = record_data['HD']['Grant Date']
        expired_date = record_data['HD']['Expired Date']
        cancellation_date = record_data['HD']['Cancellation Date']
        operator_class = record_data['AM']['Operator Class'] if 'AM' in record_data else ''
        if (operator_class is not None) and (operator_class != '') and (operator_class != ' '):
            operator_class = OPERATOR_CLASS_CODES[operator_class]
        group_code = record_data['AM']['Group Code'] if 'AM' in record_data else ''
        trustee_call_sign = record_data['AM']['Trustee Call Sign'] if 'AM' in record_data else ''
        trustee_name = record_data['AM']['Trustee Name'] if 'AM' in record_data else ''
        previous_call_sign = record_data['AM']['Previous Call Sign'] if 'AM' in record_data else ''
        region_code = record_data['AM']['Region Code'] if 'AM' in record_data else ''
        vanity = record_data['AM']['Vanity Call Sign Change'] if 'AM' in record_data else ''
        systematic = record_data['AM']['Systematic Call Sign Change'] if 'AM' in record_data else ''
        license_record = LicenseRecord(
            call_sign=call_sign,
            status=status,
            frn=frn,
            raw=record_data,
            system_identifier=usi,
            first_name=first_name,
            middle_initial=middle_initial,
            last_name=last_name,
            street_address=street_address,
            attn_line=attn_line,
            city=city,
            state=state,
            zip_code=zip_code,
            po_box=po_box,
            grant_date=grant_date,
            expired_date=expired_date,
            cancellation_date=cancellation_date,
            operator_class=operator_class,
            group_code=group_code,
            trustee_call_sign=trustee_call_sign,
            trustee_name=trustee_name,
            previous_call_sign=previous_call_sign,
            region_code=region_code,
            vanity=vanity,
            systematic=systematic,
        )
        license_records[usi] = license_record
    return license_records


def records_by_call_sign(license_records: dict[str, LicenseRecord]) -> dict[str, list[LicenseRecord]]:
    call_sign_records: dict[str, list[LicenseRecord]] = {}
    for usi, license_record in license_records.items():
        call_sign = license_record.call_sign
        if call_sign not in call_sign_records:
            call_sign_records[call_sign] = [license_record]
        else:
            call_sign_records[call_sign].append(license_record)
    return call_sign_records


class LicenseRecord(typing.NamedTuple):
    call_sign: str
    status: str
    frn: str | None
    system_identifier: str
    first_name: str | None
    middle_initial: str | None
    last_name: str | None
    street_address: str | None
    attn_line: str | None
    city: str | None
    state: str | None
    zip_code: str | None
    po_box: str | None
    grant_date: str | None
    expired_date: str | None
    cancellation_date: str | None
    operator_class: str | None
    group_code: str | None
    trustee_call_sign: str | None
    trustee_name: str | None
    previous_call_sign: str | None
    region_code: str | None
    vanity: str | None
    systematic: str | None
    raw: dict[str, Any]

    @property
    def call_sign_morse(self) -> str:
        return ' '.join(MORSE_TABLE[c] for c in self.call_sign)

    @property
    def morse_dits(self) -> int:
        return self.call_sign_morse.count('.')

    @property
    def morse_dahs(self) -> int:
        return self.call_sign_morse.count('-')

    @property
    def format(self) -> str:
        pattern = r'([A-Z]+)\d([A-Z]+)'
        match = re.match(pattern, self.call_sign)
        if not match:
            return ''
        prefix, suffix = match.groups()
        return f'{len(prefix)}x{len(suffix)}'

    @property
    def phonetic(self) -> str:
        return ' '.join(PHONETIC_WORDS[c] for c in self.call_sign)

    @property
    def syllable_length(self) -> int:
        return self.get_syllable_length()

    def get_syllable_length(self, lengths: dict[str, int] | None = None) -> int:
        if lengths is None:
            lengths = SYLLABLE_LENGTHS
        return sum(lengths[c] for c in self.call_sign)

    @property
    def fcc_uls_link(self) -> str:
        return f'https://wireless2.fcc.gov/UlsApp/UlsSearch/license.jsp?licKey={self.system_identifier}'

    @property
    def qrz_call_sign_link(self) -> str:
        return f'https://www.qrz.com/db/{self.call_sign}'

    def as_dict(self) -> dict[str, str | int | None]:
        return {
            'call_sign': self.call_sign,
            'status': self.status,
            'frn': self.frn,
            'system_identifier': self.system_identifier,
            'first_name': self.first_name,
            'middle_initial': self.middle_initial,
            'last_name': self.last_name,
            'street_address': self.street_address,
            'attn_line': self.attn_line,
            'city': self.city,
            'state': self.state,
            'zip_code': self.zip_code,
            'po_box': self.po_box,
            'grant_date': self.grant_date,
            'expired_date': self.expired_date,
            'cancellation_date': self.cancellation_date,
            'operator_class': self.operator_class,
            'group_code': self.group_code,
            'trustee_call_sign': self.trustee_call_sign,
            'trustee_name': self.trustee_name,
            'previous_call_sign': self.previous_call_sign,
            'region_code': self.region_code,
            'vanity': self.vanity,
            'systematic': self.systematic,
            'call_sign_morse': self.call_sign_morse,
            'morse_dits': self.morse_dits,
            'morse_dahs': self.morse_dahs,
            'format': self.format,
            'phonetic': self.phonetic,
            'syllable_length': self.syllable_length,
            'fcc_uls_link': self.fcc_uls_link,
            'qrz_call_sign_link': self.qrz_call_sign_link,
        }
