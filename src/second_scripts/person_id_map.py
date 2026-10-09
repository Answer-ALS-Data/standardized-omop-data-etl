"""Validate all person links before remapping any table; never invent orphan IDs."""
import json
from pathlib import Path
from identity import IdentityError, integer_id, read_table, write_table

TABLES = ('procedure_occurrence', 'observation', 'visit_occurrence',
          'condition_occurrence', 'measurement', 'device_exposure', 'death',
          'drug_exposure', 'observation_period')


def remap(input_dir='combined_omop'):
    root = Path(input_dir)
    columns, people = read_table(root/'person.csv')
    if 'person_id' not in columns:
        raise IdentityError('PERSON lacks person_id')
    mapping = {}
    for ordinal, row in enumerate(people, 1):
        key = integer_id(row['person_id'])
        if key in mapping:
            raise IdentityError('Duplicate source identity in PERSON')
        # Preserve the existing allocation convention pending the release-ID decision.
        mapping[key] = str(ordinal)
    tables = {'person.csv': (columns, people)}
    errors = []
    for table in TABLES:
        path = root/(table+'.csv')
        if not path.exists():
            continue
        cols, rows = read_table(path)
        if 'person_id' not in cols:
            raise IdentityError(f'{path.name} lacks person_id')
        for ordinal, row in enumerate(rows, 1):
            try:
                key = integer_id(row['person_id'])
                if key not in mapping:
                    raise IdentityError('Source identity absent from PERSON')
                row['person_id'] = mapping[key]
            except IdentityError as exc:
                errors.append({'table':path.name, 'record_ordinal':ordinal, 'reason':str(exc)})
        tables[path.name] = (cols, rows)
    report = {'status':'blocked' if errors else 'passed', 'person_rows':len(people),
              'allocation':'existing PERSON row order; cross-release policy unresolved',
              'table_rows':{n:len(rows) for n,(_,rows) in tables.items()},
              'unresolved_links':errors, 'clinical_rows_dropped':0}
    (root/'identity-validation.json').write_text(json.dumps(report, indent=2)+'\n')
    if errors:
        raise IdentityError(f'{len(errors)} unresolved person links; see identity-validation.json; no CSVs changed')
    for row in people:
        row['person_id'] = mapping[integer_id(row['person_id'])]
    for name, (cols, rows) in tables.items():
        write_table(root/name, cols, rows)
    write_table(root/'person_id_mapping.csv', ['Participant_ID','person_id'],
                [{'Participant_ID':key,'person_id':value} for key,value in mapping.items()])
    return report


if __name__ == '__main__':
    remap()
