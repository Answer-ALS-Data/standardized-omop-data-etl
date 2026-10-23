import importlib.util
import os
from pathlib import Path
import sys,tempfile,unittest,csv
ROOT=Path(__file__).resolve().parents[1]
class NoResponsesAndVisits(unittest.TestCase):
 def load(self,path):
  spec=importlib.util.spec_from_file_location('tested_module',ROOT/'src'/path);mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod);return mod
 def test_yes_no_unknown_are_distinct(self):
  with tempfile.TemporaryDirectory() as d:
   cwd=Path.cwd();os.chdir(d);Path('logs').mkdir();sys.path.insert(0,str(ROOT/'src/table_scripts'))
   try:
    mod=self.load('table_scripts/environmental_questionnaire--observation.py')
    for f in [mod.smokerb_to_concept_id,mod.milirb_to_concept_id]:
     self.assertEqual(f(1),45877994);self.assertEqual(f(2),45878245)
     for value in [0,None,float('nan'),9]:self.assertIsNone(f(value))
   finally:sys.path.pop(0);os.chdir(cwd)
 def test_final_visit_references_cleared_and_staging_preserved(self):
  sys.path.insert(0,str(ROOT/'src/second_scripts'))
  try:
   mod=self.load('second_scripts/transform_ids.py')
   with tempfile.TemporaryDirectory() as d:
    p=Path(d)/'measurement.csv';p.write_text('measurement_id,person_id,visit_occurrence_id,visit_detail_id,value_source_value\n1,2,SUBJECT_0,123,source evidence\n')
    before=p.read_bytes();cols,rows=mod.prepare_table('measurement',d)
    self.assertEqual(rows[0]['visit_occurrence_id'],'');self.assertEqual(rows[0]['visit_detail_id'],'')
    self.assertEqual(rows[0]['value_source_value'],'source evidence');self.assertEqual(p.read_bytes(),before)
  finally:sys.path.pop(0)
