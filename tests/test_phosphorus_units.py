import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
class PhosphorusUnitsTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.cwd=Path.cwd();os.chdir(self.tmp.name);Path('logs').mkdir()
        sys.path.insert(0,str(ROOT/'src/table_scripts'))
        spec=importlib.util.spec_from_file_location('phosphorus_units',ROOT/'src/table_scripts/auxiliary_chemistry_labs--measurement.py');self.mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.mod)
    def tearDown(self):
        sys.path.pop(0);os.chdir(self.cwd);self.tmp.cleanup()
    def row(self,value,unit,norm=1):
        return dict(Participant_ID='SYNTHETIC',labdt=0,acuarslt=float('nan'),accrrslt=float('nan'),acckrslt=float('nan'),acphrslt=value,acphouni=unit,phonorm=norm)
    def convert(self,rows):return self.mod.auxiliary_chemistry_labs_to_measurement(pd.DataFrame(rows),'2016-01-01')
    def test_conversion_preserves_source_evidence(self):
        r=self.convert([self.row(40,'mg/L')]).iloc[0]
        self.assertEqual(r.value_as_number,4);self.assertEqual(r.unit_concept_id,8840)
        self.assertIn('(lab result): 40',r.value_source_value);self.assertTrue(r.unit_source_value.endswith('mg/L'))
        self.assertEqual(r.measurement_date,'2016-01-01')
    def test_only_explicit_mg_per_liter_changes(self):
        r=self.convert([self.row(40,u) for u in ['mg/L',' MG / l ','mg/dL','2.7 - 4.5 mg/dL','unknown','mg/L extra']])
        self.assertEqual(list(r.value_as_number),[4,4,40,40,40,40])
    def test_zero_fraction_and_existing_retention(self):
        r=self.convert([self.row(0,'mg/L'),self.row(3.8,'mg/L'),self.row(float('nan'),'mg/L'),self.row(40,float('nan')),self.row(40,'mg/L',float('nan')),self.row('mg/dL','3.4')])
        self.assertEqual(list(r.value_as_number),[0,0.38,4])
    def test_other_analytes_unchanged(self):
        r=self.row(40,'mg/L');r.update(acuarslt=40,acuaunit='mg/L',uanorm=1)
        out=self.convert([r]);self.assertEqual(list(out.value_as_number),[40,4])

    def test_missing_normality_is_unknown_for_all_analytes(self):
        for result, unit, norm in [('acuarslt','acuaunit','uanorm'),('accrrslt','accreuni','crenorm'),('acphrslt','acphouni','phonorm'),('acckrslt','acckunit','cknorm')]:
            for missing_column in [False, True]:
                with self.subTest(analyte=result, missing_column=missing_column):
                    row=self.row(float('nan'),'mg/dL')
                    row.update({result:12,unit:'mg/dL',norm:float('nan')})
                    if missing_column: del row[norm]
                    out=self.convert([row])
                    self.assertEqual(len(out),1)
                    actual=out.iloc[0]
                    self.assertEqual(actual.value_as_number,12)
                    self.assertEqual(actual.value_as_concept_id,0)
                    self.assertEqual(actual.measurement_date,'2016-01-01')
                    self.assertIn(f'{norm} (norm status): missing (Unknown)',actual.value_source_value)
    def test_reported_normality_preserved(self):
        out=self.convert([self.row(4,'mg/dL',n) for n in [1,2,3]])
        self.assertEqual(list(out.value_as_concept_id),[4069590,4135493,4135493])
        for row,n,label in zip(out.itertuples(),[1,2,3],['Normal','Abnormal and Not Clinically Significant','Abnormal Clinically Significant']):
            self.assertIn(f'(norm status): {n} ({label})',row.value_source_value)
