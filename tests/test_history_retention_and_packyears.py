import csv
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]

def write(path,rows):
    with path.open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)

def read(path):
    with path.open(newline='') as f:return list(csv.DictReader(f))

def run(root,script):
    return subprocess.run([sys.executable,str(ROOT/'src'/script)],cwd=root,check=True,capture_output=True,env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'})

class HistoryRetentionAndPackYears(unittest.TestCase):
    def setup_root(self,root):
        for d in ['source_tables/usagi','processed_source','logs']:(root/d).mkdir(parents=True)

    def test_history_retains_unknowns_once_and_accounts_for_negatives(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);self.setup_root(root)
            rows=[dict(Participant_ID='TEST',Visit_Date=0,medhxdsc=label,medhxyr=day,medhxprs=1) for label,day in [('asbestos',-1),('unknown',0),('unknown',0),('known condition',1),('unresolved condition',2),('None',3),('',4),('unresolved observation',-736918)]]
            write(root/'source_tables/medical_history.csv',rows)
            write(root/'source_tables/usagi/medical_history_conditions_v2.csv',[
                dict(sourceName='asbestos',conceptId=44783302,domainId='Observation',conceptName='Exposure',equivalence='EQUAL'),
                dict(sourceName='known condition',conceptId=24609,domainId='Condition',conceptName='Synthetic condition target',equivalence='EQUAL'),
                dict(sourceName='unresolved condition',conceptId=0,domainId='Condition',conceptName='Unknown',equivalence='UNMATCHED'),
                dict(sourceName='unresolved observation',conceptId=0,domainId='Observation',conceptName='Unknown',equivalence='UNMATCHED')])
            (root/'source_tables/medical_history_date_profile.json').write_text('{"medical_history_units":"relative-days"}')
            run(root,'table_scripts/medical_history--observation.py')
            obs=read(root/'processed_source/medical_history--observation.csv');self.assertEqual(len(obs),4)
            self.assertEqual(obs[0]['observation_date'],'2015-12-31')
            self.assertEqual(sum(r['observation_concept_id']=='0' for r in obs),3)
            self.assertEqual(obs[-1]['observation_date'],'1900-01-01');self.assertIn('invalid',obs[-1]['observation_source_value'])
            audit=json.loads((root/'logs/medical-history-retention.json').read_text())
            self.assertEqual(audit['counts']['explicit negative; no positive event'],1)
            self.assertEqual(audit['counts']['missing description'],1)
            run(root,'table_scripts/medical_history--condition_occurrence.py')
            cond=read(root/'processed_source/medical_history--condition_occurrence.csv');self.assertEqual(len(cond),2)
            self.assertEqual([r['condition_concept_id'] for r in cond],['24609','0'])

    def test_pack_years_formula_zero_missing_and_measurement_routing(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);self.setup_root(root)
            rows=[dict(Participant_ID='P'+str(i),Visit_Date=0,yrssmktb=y,smkavgtb=p) for i,(y,p) in enumerate([(10,2),(20,.5),(0,2),('',2),(10,''),('bad',2)])]
            write(root/'source_tables/environmental_questionnaire.csv',rows)
            run(root,'table_scripts/environmental_questionnaire--observation.py')
            out=read(root/'processed_source/environmental_questionnaire--measurement.csv')
            self.assertEqual([float(r['value_as_number']) for r in out],[20,10,0])
            self.assertTrue(all(r['measurement_concept_id']=='903650' for r in out))
            self.assertIn('10',out[0]['value_source_value'])
            self.assertEqual(read(root/'processed_source/environmental_questionnaire--observation.csv'),[])

    def test_combination_preserves_unknowns_and_new_sources(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);self.setup_root(root)
            write(root/'processed_source/aalshxfx--observation.csv',[dict(person_id='P',observation_concept_id=0,observation_source_value='prior unknown')])
            write(root/'processed_source/medical_history--observation.csv',[dict(person_id='P',observation_concept_id=0,observation_source_value=x) for x in ['unknown A','unknown B']])
            write(root/'processed_source/environmental_questionnaire--measurement.csv',[dict(person_id='P',measurement_concept_id=903650,value_as_number=20)])
            run(root,'second_scripts/combine_subtables.py')
            self.assertEqual(len(read(root/'combined_omop/observation.csv')),3)
            self.assertEqual(read(root/'combined_omop/measurement.csv')[0]['value_as_number'],'20')

if __name__=='__main__':unittest.main()
