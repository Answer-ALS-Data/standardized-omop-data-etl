"""Prepare original or NeuroBank-prefixed CSVs without changing clinical values.

This is input compatibility only: dates, duplicates, mappings and cohort policy
are not corrected. Use a fresh private output directory outside the source tree.
"""
import argparse
import csv
import hashlib
import io
import json
from pathlib import Path
import shutil
import tempfile

import pandas as pd

ALIASES = {'Visit Date': 'Visit_Date', 'Visit Name': 'Visit_Name',
           'Form Name': 'Form_Name', 'Child Name': 'Child_Name',
           'Participant ID': 'Participant_ID'}
ALSFRS_NUMERIC = [f'alsfrs{i}' for i in range(1, 10) if i != 5] + [
    'alsfrs5a', 'alsfrs5b', 'alsfrsr1', 'alsfrsr2', 'alsfrsr3', 'alsfrst', 'alsfrsdt']
# Required core fields, plus the columns the legacy readers explicitly type.
# Optional mapping fields remain optional; this is not a clinical dictionary.
REQUIRED = {
    'subjects.csv': ['subject_group_id'],
    'demographics.csv': ['dob', 'sex', 'ethnic', 'raceamin', 'raceasn', 'raceblk', 'racenh', 'racewt'],
    'aalsdxfx.csv': ['Visit_Date', 'alsdxdt'],
    'aalshxfx.csv': ['Visit_Date', 'onsetdt'],
    'als_gene_mutations.csv': ['Visit_Date'],
    'alsfrs_r.csv': ALSFRS_NUMERIC,
    'answer_als_medications_log.csv': ['med', 'medenddt', 'medstdt', 'medfreq', 'medrte', 'medu'],
    'auxiliary_chemistry.csv': ['labdt'],
    'auxiliary_chemistry_labs.csv': ['labdt'],
    'environmental_questionnaire.csv': ['Visit_Date'],
    'family_history_log.csv': ['Visit_Date', 'famrel'],
    'medical_history.csv': ['medhxdsc', 'medhxyr', 'medhxprs'],
    'neurolog.csv': ['date1', 'hidden2'],
    'vital_signs.csv': ['vsdt'],
}
TYPED = {'alsfrs_r.csv': ALSFRS_NUMERIC, 'als_gene_mutations.csv': ['Visit_Date'],
         'vital_signs.csv': ['vsdt']}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical_name(name):
    return name.lower().removeprefix('v_nb_iati_')


def parse(data, label):
    records = list(csv.reader(io.StringIO(data.decode('utf-8-sig'), newline='')))
    if not records or not records[0]:
        raise ValueError(f'{label}: empty CSV')
    original = records[0]
    headers = [ALIASES.get(h, h) for h in original]
    if any(not h for h in headers) or len(set(headers)) != len(headers):
        raise ValueError(f'{label}: empty, duplicate or colliding aliased headers')
    if any(len(row) != len(headers) for row in records[1:]):
        raise ValueError(f'{label}: inconsistent CSV row width')
    return original, headers, [dict(zip(headers, row)) for row in records[1:]]


def key(value):
    # Opaque identifiers: never coerce to numbers or remove leading zeroes.
    value = value.strip()
    if value in ('', 'NA'):
        raise ValueError('Blank identifier')
    return value


def crosswalk_from(data):
    _, headers, rows = parse(data, 'crosswalk')
    if not {'SubjectUID', 'Participant_ID'} <= set(headers):
        raise ValueError('Crosswalk requires SubjectUID and Participant_ID')
    forward, reverse = {}, {}
    for row in rows:
        uid, pid = key(row['SubjectUID']), key(row['Participant_ID'])
        if uid in forward and forward[uid] != pid:
            raise ValueError('Ambiguous SubjectUID in crosswalk')
        if pid in reverse and reverse[pid] != uid:
            raise ValueError('Participant_ID belongs to multiple SubjectUIDs')
        forward[uid], reverse[pid] = pid, uid
    return forward


def prepare(source, output, crosswalk=None, medical_history_units=None):
    source, output = Path(source).resolve(), Path(output).resolve()
    crosswalk = Path(crosswalk).resolve() if crosswalk else None
    if medical_history_units not in ('years', 'relative-days'):
        raise ValueError('Explicit medical-history units are required; no inference from filenames')
    if output.exists() or output.is_relative_to(source) or source.is_relative_to(output):
        raise ValueError('Use a fresh output directory outside the source tree')
    if crosswalk and (crosswalk.is_relative_to(output) or output == crosswalk):
        raise ValueError('Output overlaps the crosswalk')
    candidates = sorted(p for p in source.iterdir() if p.is_file() and p.suffix.lower() == '.csv')
    files = {}
    for path in candidates:
        name = canonical_name(path.name)
        if name in files:
            raise ValueError(f'Multiple input files resolve to {name}')
        files[name] = path
    missing = sorted(set(REQUIRED)-files.keys())
    if missing:
        raise ValueError('Missing required tables: ' + ', '.join(missing))
    cw_bytes = crosswalk.read_bytes() if crosswalk else None
    mapping = crosswalk_from(cw_bytes) if cw_bytes is not None else None
    report = {'medical_history_units': medical_history_units,
              'scope': 'Compatibility only; no date conversion, deduplication or mapping changes',
              'row_locator': 'Source CSV record ordinal after header, preserved one-to-one in prepared CSV',
              'identifier_matching': 'Trim surrounding whitespace for lookup only; preserve source values; no numeric coercion',
              'preparer_sha256': digest(Path(__file__).read_bytes()),
              'crosswalk': {'path': str(crosswalk), 'sha256': digest(cw_bytes), 'unique_subjects': len(mapping)} if crosswalk else None,
              'files': []}
    prepared = []
    observed_uids, observed_pids = {}, {}
    for name, path in sorted(files.items()):
        data = path.read_bytes()
        original, headers, rows = parse(data, name)
        ancillary = name.startswith('__clinical_dictionary_')
        add_id = not ancillary and 'Participant_ID' not in headers
        if not ancillary:
            if add_id and ('SubjectUID' not in headers or mapping is None):
                raise ValueError(f'{name}: SubjectUID and a pinned crosswalk are required to add Participant_ID')
            for ordinal, row in enumerate(rows, 1):
                try:
                    uid = key(row['SubjectUID']) if 'SubjectUID' in headers else None
                    if mapping is not None and uid is not None and uid not in mapping:
                        raise ValueError('SubjectUID absent from crosswalk')
                    if add_id:
                        row['Participant_ID'] = mapping[uid]
                    pid = key(row['Participant_ID'])
                    if mapping is not None and uid is not None and mapping[uid] != pid:
                        raise ValueError('Participant_ID conflicts with crosswalk')
                    if uid is not None:
                        if uid in observed_uids and observed_uids[uid] != pid:
                            raise ValueError('Conflicting Participant_ID across source rows')
                        if pid in observed_pids and observed_pids[pid] != uid:
                            raise ValueError('Participant_ID linked to multiple SubjectUIDs')
                        observed_uids[uid], observed_pids[pid] = pid, uid
                except ValueError as exc:
                    raise ValueError(f'{name}, CSV record {ordinal}: {exc}') from None
            if add_id:
                headers.append('Participant_ID')
            absent = set(REQUIRED.get(name, []))-set(headers)
            if absent:
                raise ValueError(f'{name}: missing required columns: ' + ', '.join(sorted(absent)))
        if original == headers:
            out_bytes = data
        else:
            buffer = io.StringIO(newline='')
            writer = csv.DictWriter(buffer, fieldnames=headers)
            writer.writeheader(); writer.writerows(rows)
            out_bytes = buffer.getvalue().encode('utf-8')
        # Exercise the same parsing conventions as legacy readers without
        # replacing source strings or treating parse success as data validity.
        try:
            frame = pd.read_csv(io.BytesIO(out_bytes), dtype={c:'Int64' for c in TYPED.get(name, [])})
        except (ValueError, TypeError, OverflowError) as exc:
            raise ValueError(f'{name}: legacy CSV parsing failed ({type(exc).__name__}); source values withheld') from None
        if len(frame) != len(rows):
            raise ValueError(f'{name}: parser changed row count')
        prepared.append((name, out_bytes))
        report['files'].append({'source': str(path), 'canonical_name': name,
            'source_sha256': digest(data), 'prepared_sha256': digest(out_bytes),
            'rows': len(rows), 'headers': headers, 'participant_id_added': add_id,
            'header_aliases': {a:b for a,b in zip(original,headers) if a!=b},
            'typed_columns': TYPED.get(name, []), 'byte_preserved': data==out_bytes})
    report['linked_unique_subjects'] = len(observed_uids)
    report['all_sources_unchanged'] = all(digest(Path(f['source']).read_bytes())==f['source_sha256'] for f in report['files'])
    if crosswalk and digest(crosswalk.read_bytes()) != report['crosswalk']['sha256']:
        raise ValueError('Crosswalk changed during preparation')
    if not report['all_sources_unchanged']:
        raise ValueError('Source changed during preparation')
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix='.prepare-', dir=output.parent))
    try:
        (temporary/'clinical').mkdir()
        for name, data in prepared:
            (temporary/'clinical'/name).write_bytes(data)
        (temporary/'preparation.json').write_text(json.dumps(report, indent=2, sort_keys=True)+'\n')
        if output.exists():
            raise ValueError('Output appeared during preparation; refusing replacement')
        temporary.rename(output)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', required=True, type=Path)
    p.add_argument('--output', required=True, type=Path)
    p.add_argument('--crosswalk', type=Path)
    p.add_argument('--medical-history-units', required=True, choices=['years','relative-days'])
    a = p.parse_args()
    try:
        report = prepare(a.source, a.output, a.crosswalk, a.medical_history_units)
    except ValueError as exc:
        p.error(str(exc))
    print(json.dumps({'files':len(report['files']), 'linked_unique_subjects':report['linked_unique_subjects'],
                      'all_sources_unchanged':report['all_sources_unchanged'],
                      'medical_history_units':report['medical_history_units']}, indent=2))


if __name__ == '__main__':
    main()
