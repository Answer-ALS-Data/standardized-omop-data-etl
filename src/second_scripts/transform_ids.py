"""Format numeric keys without decimal stripping, truncation or invented IDs."""
from pathlib import Path
from identity import final_id, read_table, write_table

TABLES = ('condition_occurrence', 'death', 'device_exposure', 'drug_exposure',
          'measurement', 'observation_period', 'observation', 'person',
          'procedure_occurrence', 'visit_occurrence')

# Compatibility name used by callers/tests.
transform_id = final_id


def legacy_visit_id(value):
    # Composite visit tokens have a separate unresolved design (plan Step 8).
    # Preserve historical nonblank handling here; this is not visit validation.
    if not str(value).strip():
        return ''
    digits = ''.join(filter(str.isdigit, str(value)))[-7:]
    return '11' + digits.zfill(7)


def prepare_table(table_name, input_dir):
    path = Path(input_dir)/(table_name+'.csv')
    if not path.exists():
        return None
    columns, rows = read_table(path)
    # A set prevents PERSON.person_id being transformed twice.
    keys = {table_name+'_id', 'person_id'} & set(columns)
    keys.discard('visit_occurrence_id')
    for row in rows:
        for key in keys:
            row[key] = final_id(row[key])
        if 'visit_occurrence_id' in columns:
            row['visit_occurrence_id'] = legacy_visit_id(row['visit_occurrence_id'])
    return columns, rows


def transform_table_ids(table_name, input_dir='combined_omop', output_dir='final_omop'):
    result = prepare_table(table_name, input_dir)
    if result is not None:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        write_table(Path(output_dir)/(table_name+'.csv'), *result)


def main(input_dir='combined_omop', output_dir='final_omop'):
    # Validate every numeric key before publishing any of these tables.
    prepared = {name:prepare_table(name,input_dir) for name in TABLES}
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    for name,result in prepared.items():
        if result is not None:
            write_table(Path(output_dir)/(name+'.csv'), *result)


if __name__ == '__main__':
    main()
