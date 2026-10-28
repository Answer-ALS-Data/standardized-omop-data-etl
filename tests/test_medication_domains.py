"""Medication mapping domain routing and preservation across source tables."""
import csv
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]

def write(path, rows):
    with path.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

def read(path):
    with path.open(newline='') as f:
        return list(csv.DictReader(f))

class MedicationDomainsTest(unittest.TestCase):
    def test_device_dates_evidence_and_unresolved_drug_retention(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for d in ('source_tables/usagi', 'logs', 'processed_source'):
                (root/d).mkdir(parents=True)
            rows = [dict(Participant_ID='P1', med=name, medrte=99, medrtesp='uterine', medu=99, meduotsp='device', medfreq=1, meddose=1, medstdt=start, medenddt=end) for name,start,end in [('IUD',-10,20),('unknown','',''),('IUD','',''),('IUD',10,''),('IUD','',20)]]
            write(root/'source_tables/answer_als_medications_log.csv',rows)
            write(root/'source_tables/usagi/medications_v2.csv',[
                dict(sourceName='IUD',conceptId=40355609,equivalence='WIDER',domainId='Device'),
                dict(sourceName='unknown',conceptId=0,equivalence='UNMATCHED',domainId='Drug')])
            subprocess.run([sys.executable,str(ROOT/'src/table_scripts/answer_als_medications_log--drug_exposure.py')],cwd=root,check=True,capture_output=True,env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'})
            drugs=read(root/'processed_source/answer_als_medications_log--drug_exposure.csv')
            devices=read(root/'processed_source/answer_als_medications_log--device_exposure.csv')
            self.assertEqual(len(drugs),1)
            self.assertEqual(drugs[0]['drug_concept_id'],'0')
            self.assertIn('UNMATCHED',drugs[0]['drug_source_value'])
            self.assertEqual([(d['device_exposure_start_date'],d['device_exposure_end_date']) for d in devices],[('2015-12-22','2016-01-21'),('1900-01-01','1900-01-01'),('2016-01-11','2016-01-11'),('2016-01-21','2016-01-21')])
            self.assertTrue(all(d['device_concept_id']=='40355609' and d['visit_occurrence_id']=='P1_0' for d in devices))
            self.assertIn('uterine',devices[0]['device_source_value'])
            self.assertIn('WIDER',devices[0]['device_source_value'])
            history=dict(devices[0],device_concept_id='123',device_source_value='history evidence')
            write(root/'processed_source/medical_history--device_exposure.csv',[history])
            for _ in range(2):
                subprocess.run([sys.executable,str(ROOT/'src/second_scripts/combine_subtables.py')],cwd=root,check=True,capture_output=True)
                self.assertFalse((root/'combined_omop/device_exposure.csv').exists())
                combined_drugs=read(root/'combined_omop/drug_exposure.csv')
                self.assertEqual(len(combined_drugs),1)
                self.assertEqual(combined_drugs[0]['drug_concept_id'],'0')

if __name__=='__main__':
    unittest.main()
