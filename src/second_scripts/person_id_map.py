"""Use a pinned person-ID registry and narrowly reviewed event exclusions."""
import hashlib
import json
import re
from pathlib import Path
from identity import IdentityError, source_id, integer_id, final_id, read_table, write_table

TABLES = ('procedure_occurrence', 'observation', 'visit_occurrence',
          'condition_occurrence', 'measurement', 'device_exposure', 'death',
          'drug_exposure', 'observation_period')


def registry(path):
    if path is None:
        return {}
    cols, rows = read_table(path)
    if cols != ['Participant_ID', 'person_id']:
        raise IdentityError('Registry must contain Participant_ID,person_id')
    result = {}; used = set()
    for row in rows:
        key = source_id(row['Participant_ID'])
        published = row['person_id']
        if not re.fullmatch(r'11[0-9]{7}', published):
            raise IdentityError('Registry ID must be in the nine-digit 11 namespace')
        value = integer_id(published[2:])
        if key in result or value in used:
            raise IdentityError('Registry contains duplicate source or numeric identities')
        result[key] = value; used.add(value)
    return result


def exclusion_rules(path):
    if path is None:
        return []
    data = json.loads(Path(path).read_text())
    if data.get('version') != 1 or not isinstance(data.get('exclusions'), list):
        raise IdentityError('Unsupported exclusion policy')
    rules = data['exclusions']; seen = set()
    for rule in rules:
        if (rule.get('table') not in {t+'.csv' for t in TABLES}
                or not isinstance(rule.get('reason'), str) or not rule['reason'].strip()
                or not isinstance(rule.get('approved_on'), str) or not rule['approved_on']
                or type(rule.get('expected_rows')) is not int or rule['expected_rows'] < 1):
            raise IdentityError('Incomplete exclusion rule')
        key = (rule['table'], source_id(rule['Participant_ID']))
        if key in seen:
            raise IdentityError('Duplicate exclusion rule')
        seen.add(key)
    return rules


def remap(input_dir='combined_omop', registry_path=None, exclusions_path=None):
    root = Path(input_dir)
    columns, people = read_table(root/'person.csv')
    if 'person_id' not in columns:
        raise IdentityError('PERSON lacks person_id')
    sources = [source_id(row['person_id']) for row in people]
    if len(sources) != len(set(sources)):
        raise IdentityError('Duplicate source identity in PERSON')
    reserved = registry(registry_path)
    original_keys = set(reserved)
    next_id = max((int(v) for v in reserved.values()), default=0)
    for key in sorted(set(sources) - original_keys):
        next_id += 1
        final_id(next_id)  # Enforce namespace capacity before any writes.
        reserved[key] = str(next_id)
    mapping = {key:reserved[key] for key in sources}
    rules = exclusion_rules(exclusions_path)
    hits = [0] * len(rules)
    for rule in rules:
        if source_id(rule['Participant_ID']) in mapping:
            raise IdentityError('Excluded participant now has PERSON; review exclusion before proceeding')
    tables = {'person.csv': (columns, people)}
    errors = []; excluded = []; input_hashes = {}
    for table in TABLES:
        path = root/(table+'.csv')
        if not path.exists():
            continue
        input_hashes[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
        cols, rows = read_table(path)
        if 'person_id' not in cols:
            raise IdentityError(f'{path.name} lacks person_id')
        retained = []
        for ordinal, row in enumerate(rows, 1):
            try:
                key = source_id(row['person_id'])
                matching = [i for i,r in enumerate(rules) if r['table']==path.name and source_id(r['Participant_ID'])==key]
                if matching:
                    i = matching[0]; hits[i] += 1
                    excluded.append({'table':path.name, 'record_ordinal':ordinal,
                                     'rule_index':i, 'reason':rules[i]['reason']})
                    continue
                if key not in mapping:
                    raise IdentityError('Source identity absent from PERSON')
                row['person_id'] = mapping[key]
            except IdentityError as exc:
                errors.append({'table':path.name, 'record_ordinal':ordinal, 'reason':str(exc)})
            retained.append(row)
        tables[path.name] = (cols, retained)
    for i, rule in enumerate(rules):
        if hits[i] != rule['expected_rows']:
            errors.append({'rule_index':i, 'reason':'Exclusion count differs from reviewed expected_rows', 'observed_rows':hits[i]})
    report = {'status':'blocked' if errors else 'passed', 'person_rows':len(people),
              'allocation':'preserve registry IDs; allocate sorted new source IDs above reserved maximum',
              'reused_ids':len(set(sources)&original_keys), 'new_ids':len(set(sources)-original_keys),
              'historical_ids_reserved':len(original_keys),
              'policy_hashes':{name:hashlib.sha256(Path(p).read_bytes()).hexdigest()
                               for name,p in [('registry',registry_path),('exclusions',exclusions_path)] if p is not None},
              'input_table_hashes':input_hashes,
              'table_rows':{n:len(rows) for n,(_,rows) in tables.items()},
              'unresolved_links':errors, 'reviewed_exclusions':excluded,
              'clinical_rows_dropped':0 if errors else len(excluded)}
    (root/'identity-validation.json').write_text(json.dumps(report, indent=2)+'\n')
    if errors:
        raise IdentityError(f'{len(errors)} unresolved identity/policy checks; see identity-validation.json; no CSVs changed')
    for row in people:
        row['person_id'] = mapping[source_id(row['person_id'])]
    for name, (cols, rows) in tables.items():
        write_table(root/name, cols, rows)
    write_table(root/'person_id_mapping.csv', ['Participant_ID','person_id'],
                [{'Participant_ID':key,'person_id':value} for key,value in mapping.items()])
    # Retain absent historical people as reservations; reuse this for later builds.
    write_table(root/'person_id_registry.csv', ['Participant_ID','person_id'],
                [{'Participant_ID':key,'person_id':final_id(value)} for key,value in sorted(reserved.items())])
    return report


if __name__ == '__main__':
    remap(registry_path='source_tables/identity/person_id_registry.csv',
          exclusions_path='source_tables/identity/exclusions.json')
