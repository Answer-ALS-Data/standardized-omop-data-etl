"""Aggregate baseline diagnostics without exporting participant rows or identifiers."""
import argparse
from collections import Counter
import csv
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path


def read(path):
    with path.open(encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        return reader.fieldnames or [], list(reader)


def canonical(value):
    s = (value or '').strip()
    if not s:
        return ''
    try:
        n = Decimal(s)
        if n.is_finite():
            return str(n.normalize()) if n else '0'
    except InvalidOperation:
        pass
    return s


def tables(directory):
    return {p.name.lower(): read(p) for p in sorted(directory.glob('*.csv'))}


def identity(data):
    rows = data.get('person.csv', ([], []))[1]
    lookup = {}
    marker = 'demographics+Participant_ID (participant identifier): '
    for row in rows:
        text = row.get('person_source_value', '')
        if marker in text:
            lookup[canonical(row.get('person_id'))] = text.split(marker, 1)[1].split(' |', 1)[0]
    return lookup


def diagnostics(data):
    persons = data.get('person.csv', ([], []))[1]
    person_ids = {canonical(r.get('person_id')) for r in persons if canonical(r.get('person_id'))}
    result = {}
    for name, (cols, rows) in data.items():
        item = dict(rows=len(rows), columns=cols)
        if 'person_id' in cols:
            ids = [canonical(r.get('person_id')) for r in rows]
            item.update(missing_person_id_rows=sum(not v for v in ids),
                        person_fk_missing_rows=sum(bool(v) and v not in person_ids for v in ids),
                        distinct_nonempty_person_ids=len(set(ids)-{''}))
        if 'visit_occurrence_id' in cols:
            visits = {canonical(r.get('visit_occurrence_id')) for r in data.get('visit_occurrence.csv', ([],[]))[1]}
            item['unresolved_nonempty_visit_reference_rows'] = sum(
                bool(canonical(r.get('visit_occurrence_id'))) and canonical(r.get('visit_occurrence_id')) not in visits
                for r in rows)
        result[name] = item
    return result


def compare(left, right):
    left_ids, right_ids = identity(left), identity(right)
    result = {}
    for name in sorted(left.keys() | right.keys()):
        lc, lr = left.get(name, ([], [])); rc, rr = right.get(name, ([], []))
        ignore = {name.removesuffix('.csv')+'_id', 'visit_occurrence_id'} - {'person_id'}
        common = sorted(set(lc)&set(rc)-ignore)
        def bag(rows, mapping):
            counts = Counter()
            for row in rows:
                values = []
                for col in common:
                    val = canonical(row.get(col))
                    if col == 'person_id':
                        val = mapping.get(val, 'UNRESOLVED:'+val)
                    values.append(val)
                counts[tuple(values)] += 1
            return counts
        a, b = bag(lr, left_ids), bag(rr, right_ids)
        result[name] = dict(left_rows=len(lr), right_rows=len(rr),
                            left_only_columns=sorted(set(lc)-set(rc)),
                            right_only_columns=sorted(set(rc)-set(lc)),
                            compared_columns=common,
                            common_row_matches=sum((a&b).values()) if common else 0,
                            left_only_rows=sum((a-b).values()) if common else len(lr),
                            right_only_rows=sum((b-a).values()) if common else len(rr))
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run', required=True, type=Path)
    p.add_argument('--released', required=True, type=Path)
    p.add_argument('--repeat', type=Path)
    p.add_argument('--output', required=True, type=Path)
    args = p.parse_args()
    run = tables(args.run/'work/final_omop'); release = tables(args.released)
    report = dict(run=str(args.run.resolve()), released=str(args.released.resolve()),
        method='Multisets on common columns; trim and normalize numeric spellings; translate person_id through each PERSON source identifier; exclude generated table primary IDs and visit IDs; report missing columns and foreign keys separately. Unresolved IDs remain unresolved, not repaired.',
        run_diagnostics=diagnostics(run), release_diagnostics=diagnostics(release),
        run_vs_release=compare(run, release),
        run_people_missing_from_release=len(set(identity(run).values())-set(identity(release).values())),
        release_people_missing_from_run=len(set(identity(release).values())-set(identity(run).values())))
    if args.repeat:
        a=json.loads((args.run/'manifest.json').read_text())
        b=json.loads((args.repeat/'manifest.json').read_text())
        report['repeat'] = dict(code_matches=a['code_commit']==b['code_commit'],
            environment_packages_match=a['packages']==b['packages'],
            input_hashes_match={r['staged']:r['sha256'] for r in a['inputs']}=={r['staged']:r['sha256'] for r in b['inputs']},
            stages={stage: dict(file_set_matches=set(a['stages'][stage])==set(b['stages'][stage]),
                 differing_file_hashes=[n for n in sorted(set(a['stages'][stage])|set(b['stages'][stage]))
                     if a['stages'][stage].get(n,{}).get('sha256')!=b['stages'][stage].get(n,{}).get('sha256')])
                 for stage in a['stages']})
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True)+'\n')
    print(json.dumps({n: {k:v for k,v in d.items() if k!='compared_columns'} for n,d in report['run_vs_release'].items()},indent=2))
    if 'repeat' in report: print(json.dumps(report['repeat'],indent=2))


if __name__ == '__main__':
    main()
