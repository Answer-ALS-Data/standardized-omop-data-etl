"""Explicit medical-history date profiles, sentinel policy and row-level audit."""
import csv
import hashlib
import json
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from helpers import year_to_date

ANCHOR = date(2016, 1, 1)
SENTINEL = date(1900, 1, 1)


def convert_date(value, units, legacy_value=None):
    if units not in ('years', 'relative-days'):
        raise ValueError('Explicit medical_history_units must be years or relative-days')
    raw = str(value).strip()
    missing = raw.lower() in ('', 'na', 'nan', 'none', 'null', '<na>')
    if units == 'years':
        # Preserve the historical parser, including its handling of mixed CSV types.
        result = year_to_date(value if legacy_value is None else legacy_value)
        status = 'missing' if missing else 'valid' if result != SENTINEL or raw == '1900' else 'invalid'
        return result, status
    if missing:
        return SENTINEL, 'missing'
    try:
        number = Decimal(raw)
        if not number.is_finite() or number != number.to_integral_value():
            return SENTINEL, 'invalid'
        return ANCHOR + timedelta(days=int(number)), 'valid'
    except (InvalidOperation, ValueError, OverflowError):
        return SENTINEL, 'invalid'


class DateContext:
    def __init__(self, source='source_tables/medical_history.csv',
                 profile='source_tables/medical_history_date_profile.json'):
        self.source = Path(source); self.profile = Path(profile)
        self.units = json.loads(self.profile.read_text()).get('medical_history_units')
        if self.units not in ('years', 'relative-days'):
            raise ValueError('Missing or invalid medical_history_units in explicit profile')
        with self.source.open(encoding='utf-8-sig', newline='') as stream:
            self.raw_rows = list(csv.DictReader(stream))
        self.records = []

    def convert(self, index, legacy_value):
        raw = self.raw_rows[index]['medhxyr']
        result, status = convert_date(raw, self.units, legacy_value)
        self.records.append({'source_record_ordinal':index+1, 'raw_value':raw,
                             'status':status, 'date':result.isoformat(), 'emitted_rows':0})
        label = 'Days since intake' if self.units=='relative-days' else 'Year of Diagnosis'
        provenance = f'medical_history+medhxyr ({label}): {raw or "BLANK"} | +date_status: {status}'
        if status != 'valid':
            provenance += ' (1900-01-01 placeholder; not an observed date)'
        elif self.units == 'relative-days':
            provenance += ' (synthetic intake anchor 2016-01-01)'
        return result, provenance

    def emitted(self, count):
        self.records[-1]['emitted_rows'] = count

    def save(self, domain):
        path = Path('logs')/f'medical-history-dates-{domain}.json'
        report = {'medical_history_units':self.units, 'synthetic_intake_anchor':str(ANCHOR),
                  'missing_invalid_sentinel':str(SENTINEL),
                  'source_sha256':hashlib.sha256(self.source.read_bytes()).hexdigest(),
                  'profile_sha256':hashlib.sha256(self.profile.read_bytes()).hexdigest(),
                  'input_status_counts':dict(Counter(r['status'] for r in self.records)),
                  'emitted_status_counts':{status:sum(r['emitted_rows'] for r in self.records if r['status']==status)
                                           for status in ('valid','missing','invalid')},
                  'records':self.records}
        path.write_text(json.dumps(report,indent=2)+'\n')
