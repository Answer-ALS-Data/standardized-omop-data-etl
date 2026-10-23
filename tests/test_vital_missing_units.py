import importlib.util
import os,sys,tempfile,unittest
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
class VitalMissingUnits(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.cwd=Path.cwd();os.chdir(self.tmp.name);Path('logs').mkdir();sys.path.insert(0,str(ROOT/'src/table_scripts'))
  spec=importlib.util.spec_from_file_location('vitals',ROOT/'src/table_scripts/vital_signs--measurement.py');self.mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.mod)
 def tearDown(self):sys.path.pop(0);os.chdir(self.cwd);self.tmp.cleanup()
 def row(self,day,unit):
  return dict(Participant_ID='SYNTHETIC',vsdt=day,temp=float('nan'),bpsys=float('nan'),bpdias=float('nan'),hr=70,rr=float('nan'),weight=150,height=66,weightu=unit,heightu=unit,bmi=24)
 def test_missing_units_omit_only_affected_measurements(self):
  for unit in [None,float('nan'),'']:
   with self.subTest(unit=unit):
    out=self.mod.vital_signs_to_measurement(pd.DataFrame([self.row(0,unit)]),'2016-01-01')
    self.assertEqual(set(out.measurement_source_value),{'vital_signs+hr (Heart rate)','vital_signs+bmi (BMI)'})
 def test_other_visit_does_not_supply_units(self):
  out=self.mod.vital_signs_to_measurement(pd.DataFrame([self.row(0,1),self.row(1,None),self.row(2,2)]),'2016-01-01')
  for field,expected in [('weight',[68.04,150]),('height',[167.64,66])]:
   selected=out[out.measurement_source_value.str.startswith('vital_signs+'+field+' ')]
   self.assertEqual(list(selected.value_as_number),expected)
   self.assertEqual(list(selected.measurement_date),['2016-01-01','2016-01-03'])
