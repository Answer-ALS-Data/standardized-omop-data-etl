import importlib.util
import os,sys,tempfile,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
class FamilyLineage(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.cwd=Path.cwd();os.chdir(self.tmp.name);Path('logs').mkdir();sys.path.insert(0,str(ROOT/'src/table_scripts'))
  spec=importlib.util.spec_from_file_location('family',ROOT/'src/table_scripts/family_history_log--observation.py');self.mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.mod)
 def tearDown(self):sys.path.pop(0);os.chdir(self.cwd);self.tmp.cleanup()
 def test_missing_lineage_is_unspecified(self):
  for rel in [9,10,'grandmother','grandfather']:
   for lineage in [None,float('nan'),'']:
    concept,source,equiv=self.mod.get_relative_concept(rel,lineage)
    self.assertEqual(concept,4210989);self.assertEqual(source,'BLANK');self.assertEqual(equiv,'WIDER')
 def test_explicit_lineage_and_other_relationships_unchanged(self):
  for rel,lineage,target in [(9,1,4052802),(9,2,4050942),(10,1,4052801),(10,2,4052800),(1,None,4051255)]:
   self.assertEqual(self.mod.get_relative_concept(rel,lineage)[0],target)
