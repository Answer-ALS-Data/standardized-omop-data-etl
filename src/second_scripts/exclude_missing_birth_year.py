"""Apply person eligibility after ID conversion, retaining a private exclusion audit."""
import csv
import json
from pathlib import Path


def exclude_missing_birth_year(output_dir, report_path):
    output_dir = Path(output_dir)
    tables = {}
    for path in sorted(output_dir.glob('*.csv')):
        with path.open(encoding='utf-8-sig', newline='') as handle:
            reader = csv.DictReader(handle)
            tables[path] = (reader.fieldnames, list(reader))
    _, people = tables[output_dir / 'person.csv']
    # Birth year is required. The event-date sentinel must never supply it.
    excluded = {row['person_id'] for row in people
                if row['year_of_birth'].strip().lower() in ('', 'na', 'nan', 'null')}
    report = {'reason': 'missing_year_of_birth',
              'excluded_people': [row for row in people if row['person_id'] in excluded],
              'excluded_rows_by_table': {}}
    for path, (fields, rows) in tables.items():
        if 'person_id' not in fields:
            continue
        removed = [row for row in rows if row['person_id'] in excluded]
        if removed:
            report['excluded_rows_by_table'][path.stem] = removed
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2) + '\n')
    for path, (fields, rows) in tables.items():
        if path.stem not in report['excluded_rows_by_table']:
            continue
        with path.open('w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(row for row in rows if row['person_id'] not in excluded)
    return report


if __name__ == '__main__':
    exclude_missing_birth_year('final_omop', 'logs/missing-birth-year-exclusions.json')
