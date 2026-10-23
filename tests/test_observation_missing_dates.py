import importlib.util
from pathlib import Path
import unittest
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('combine_dates',ROOT/'src/second_scripts/combine_subtables.py')
mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
class MissingObservationDates(unittest.TestCase):
 def test_missing_dates_get_placeholder_and_provenance(self):
  frame=pd.DataFrame({'observation_date':[None,float('nan'),'',' '],'observation_source_value':['source A','source B','source C',None],'value_as_concept_id':[1,2,3,4]})
  out=mod.apply_missing_observation_dates(frame)
  self.assertEqual(list(out.observation_date),['1900-01-01']*4)
  self.assertEqual(list(out.value_as_concept_id),[1,2,3,4])
  self.assertTrue(all('unknown-date placeholder' in v for v in out.observation_source_value))
  self.assertTrue(out.iloc[0].observation_source_value.startswith('source A | '))
  pd.testing.assert_frame_equal(out,mod.apply_missing_observation_dates(out))
 def test_existing_dates_including_history_are_unchanged(self):
  frame=pd.DataFrame({'observation_date':['1941-05-09','2016-01-01','1900-01-01','not-a-date'],'observation_source_value':['a','b','c','d']})
  pd.testing.assert_frame_equal(frame,mod.apply_missing_observation_dates(frame))
