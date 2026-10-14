"""Emit mapped history Observations and retain otherwise-unmapped source records."""
import json
from collections import Counter
from pathlib import Path

import pandas as pd
from helpers import get_visit_occurrence_id
from medical_history_dates import DateContext

COLUMNS = ['person_id', 'observation_concept_id', 'observation_source_value',
           'observation_date', 'observation_type_concept_id', 'visit_occurrence_id']
DOMAINS = {'Condition', 'Procedure', 'Device', 'Drug', 'Observation'}


def main():
    source = pd.read_csv('source_tables/medical_history.csv')
    mapping = pd.read_csv('source_tables/usagi/medical_history_conditions_v2.csv',
                          keep_default_na=False)
    labels = mapping['sourceName'].str.lower().str.strip()
    context = DateContext()
    output, audit = [], []
    for index, row in source.iterrows():
        # Read literal labels independently of pandas' NA conversion (e.g. "None").
        description = context.raw_rows[index]['medhxdsc']
        label = description.strip().lower()
        event_date, date_source = context.convert(index, row['medhxyr'])
        if label == 'none':
            audit.append({'source_record_ordinal':index+1, 'disposition':'explicit negative; no positive event', 'observation_rows':0})
            continue
        if label in ('', 'na', 'nan', 'null'):
            audit.append({'source_record_ordinal':index+1, 'disposition':'missing description', 'observation_rows':0})
            continue
        matches = mapping[labels == label]
        unsupported = set(matches['domainId']) - DOMAINS
        if unsupported:
            raise ValueError(f'History mapping has unsupported domains: {sorted(unsupported)}')
        if matches.empty:
            targets = [{'conceptId':0, 'equivalence':'UNMATCHED'}]
            disposition = 'unmapped source retained as Observation'
        else:
            targets = matches[matches['domainId']=='Observation'].to_dict('records')
            disposition = 'mapped Observation' if targets else 'mapped to another domain'
        present = row.get('medhxprs')
        present_text = '' if pd.isna(present) else str(int(present))
        present_meaning = 'yes' if present == 1 else 'no' if present == 2 else 'unknown'
        for target in targets:
            output.append({
                'person_id':row['Participant_ID'],
                'observation_concept_id':target['conceptId'],
                'observation_source_value':f'medical_history+medhxdsc (Description): {description} | {date_source} | medical_history+medhxprs (Still Present): {present_text} ({present_meaning}) | +equivalence (usagi omop mapping equivalence): {target["equivalence"]}',
                'observation_date':event_date,
                'observation_type_concept_id':32851,
                'visit_occurrence_id':get_visit_occurrence_id(row['Participant_ID'],row.get('Visit_Date')),
            })
        context.emitted(len(targets))
        audit.append({'source_record_ordinal':index+1, 'disposition':disposition, 'observation_rows':len(targets)})
    pd.DataFrame(output,columns=COLUMNS).to_csv('processed_source/medical_history--observation.csv',index=False)
    context.save('observation')
    Path('logs/medical-history-retention.json').write_text(json.dumps({
        'counts':dict(Counter(r['disposition'] for r in audit)), 'records':audit},indent=2)+'\n')


if __name__=='__main__':
    main()
