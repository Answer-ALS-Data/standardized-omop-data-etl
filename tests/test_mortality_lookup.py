import csv
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SCRIPT=Path(__file__).resolve().parents[1]/'src/table_scripts/mortality--death.py'
spec=importlib.util.spec_from_file_location('mortality',SCRIPT)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

def write(path,fields,rows):
    with path.open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)

class MortalityTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.cwd=Path.cwd();os.chdir(self.root)
        for d in ['source_tables/usagi','source_tables/other','processed_source']:(self.root/d).mkdir(parents=True)
        self.map=[dict(diedcaus_key='als',icd10cm_key='G12.21',cause_concept_id='373182',mapping_status='APPROVED')]
        self.save_map()
        (self.root/'source_tables/other/mortality-date-overrides.json').write_text('{}')
        write(self.root/'source_tables/alsfrs_r.csv',['Participant_ID','alsfrsdt','alsfdn'],[])
        write(self.root/'source_tables/vital_signs.csv',['Participant_ID','vsdt','vsdn'],[])
    def tearDown(self):
        os.chdir(self.cwd);self.tmp.cleanup()
    def save_map(self):write(self.root/'source_tables/usagi/mortality-cause-mapping.csv',list(self.map[0]),self.map)
    def source(self,rows):
        write(self.root/'source_tables/mortality.csv',['Participant_ID','dieddt','diedcaus','icd10cm'],[dict(Participant_ID=p,dieddt=d,diedcaus=c,icd10cm=i) for p,d,c,i in rows])
    def test_normalization_and_duplicate_provenance(self):
        self.source([('P','10',' ALS ','G 12.21'),('P','10','ALS','g12.21')]);m.main()
        output=m.read(self.root/'processed_source/mortality--death.csv');self.assertEqual(len(output),1);self.assertEqual(output[0]['death_date'],'2016-01-11')
        audit=json.loads(Path('logs/mortality-reconciliation.json').read_text());self.assertEqual(audit['records'][0]['source_rows'],[1,2])
    def test_unknown_and_duplicate_lookup_fail(self):
        self.source([('P','10','other','')])
        with self.assertRaises(ValueError):m.main()
        self.map*=2;self.save_map()
        with self.assertRaises(ValueError):m.main()
    def test_missing_negative_and_postdeath_hold(self):
        self.source([('A','','ALS','G12.21'),('B','-1','ALS','G12.21'),('C','10','ALS','G12.21')])
        write(Path('source_tables/alsfrs_r.csv'),['Participant_ID','alsfrsdt','alsfdn'],[dict(Participant_ID='C',alsfrsdt='11',alsfdn='1')]);m.main()
        self.assertEqual(m.read(Path('processed_source/mortality--death.csv')),[])
        self.assertEqual(len(json.loads(Path('logs/mortality-reconciliation.json').read_text())['records']),3)
    def test_override_guard(self):
        self.source([('P','10','ALS','G12.21')]);p=Path('source_tables/other/mortality-date-overrides.json');p.write_text(json.dumps({'P':dict(expected_raw_offset=10,retained_offset=9)}))
        write(Path('source_tables/alsfrs_r.csv'),['Participant_ID','alsfrsdt','alsfdn'],[dict(Participant_ID='P',alsfrsdt='10',alsfdn='1')]);m.main()
        self.assertIn('warning',json.loads(Path('logs/mortality-reconciliation.json').read_text())['records'][0])
        self.assertEqual(m.read(Path('processed_source/mortality--death.csv'))[0]['death_date'],'2016-01-10')
        self.source([('P','11','ALS','G12.21')])
        with self.assertRaises(ValueError):m.main()
    def test_conflicting_duplicate_fails(self):
        self.source([('P','10','ALS','G12.21'),('P','11','ALS','G12.21')])
        with self.assertRaises(ValueError):m.main()
