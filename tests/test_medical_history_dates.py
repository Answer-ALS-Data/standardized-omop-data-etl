"""Date policy and all four domain integrations with synthetic source records."""
import csv
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from datetime import date

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src/table_scripts'))
from medical_history_dates import convert_date, DateContext


class MedicalHistoryDates(unittest.TestCase):
    def test_offsets_and_leap_boundaries(self):
        for value,expected in [('0','2016-01-01'),('-1','2015-12-31'),('59','2016-02-29'),('60','2016-03-01'),('366','2017-01-01'),('-27265','1941-05-09')]:
            with self.subTest(value=value):
                result,status=convert_date(value,'relative-days')
                self.assertEqual(result.isoformat(),expected)
                self.assertEqual(status,'valid')

    def test_missing_distinct_from_invalid(self):
        for value in ['', 'NA', None, float('nan')]:
            self.assertEqual(convert_date(value,'relative-days'),(date(1900,1,1),'missing'))
        for value in ['-736918','bad','1.5','Infinity','999999999999999999999']:
            self.assertEqual(convert_date(value,'relative-days'),(date(1900,1,1),'invalid'))

    def test_units_explicit_not_magnitude(self):
        self.assertEqual(convert_date('2000','years')[0],date(2000,1,1))
        self.assertNotEqual(convert_date('2000','relative-days')[0],date(2000,1,1))
        with self.assertRaises(ValueError):convert_date('2000','auto')

    def test_legacy_year_parser_retained(self):
        self.assertEqual(convert_date('2000','years','2000')[0],date(2000,1,1))
        self.assertEqual(convert_date('0','years')[0],date(1900,1,1))
        self.assertEqual(convert_date('2000.0','years',2000.0)[0],date(1900,1,1))

    def test_explicit_invalid_profiles_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(FileNotFoundError):DateContext(profile=Path(d)/'missing.json')
            p=Path(d)/'profile.json';p.write_text('{}')
            with self.assertRaises(ValueError):DateContext(profile=p)

    def test_legacy_profile_override(self):
        with tempfile.TemporaryDirectory() as d:
            source = Path(d)/'history.csv'
            source.write_text('medhxyr\n2000\n')
            profile = Path(d)/'profile.json'
            profile.write_text(json.dumps({'medical_history_units': 'years'}))
            context = DateContext(source=source, profile=profile)
            result, provenance = context.convert(0, '2000')
            self.assertEqual(result, date(2000, 1, 1))
            self.assertIn('Year of Diagnosis', provenance)

    def test_sentinel_excluded_from_observation_periods(self):
        spec=importlib.util.spec_from_file_location('periods',ROOT/'src/second_scripts/create_observation_periods.py')
        mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
        self.assertFalse(mod.is_valid_date('1900-01-01'))
        self.assertTrue(mod.is_valid_date('2016-01-01'))
        self.assertTrue(mod.is_valid_date('2015-12-31'))

    def test_four_domains_one_to_many_and_placeholders(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            for folder in ['source_tables/usagi','logs','processed_source']:(root/folder).mkdir(parents=True)
            source=[{'Participant_ID':'SYNTHETIC','Visit_Date':'10','medhxdsc':'synthetic term','medhxyr':value,'medhxprs':'1'} for value in ['0','NA','-736918']]
            def write(path,rows):
                with path.open('w',newline='') as f:
                    w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
            write(root/'source_tables/medical_history.csv',source)
            maps=[{'sourceName':'synthetic term','domainId':domain,'conceptId':str(100+i),'conceptName':'Synthetic','equivalence':'EQUAL'} for i,domain in enumerate(['Condition','Drug','Procedure','Device']*2)]
            write(root/'source_tables/usagi/medical_history_conditions_v2.csv',maps)
            for domain,datefield in [('condition_occurrence','condition_start_date'),('drug_exposure','drug_exposure_start_date'),('procedure_occurrence','procedure_date'),('device_exposure','device_exposure_start_date')]:
                subprocess.run([sys.executable,str(ROOT/'src/table_scripts'/f'medical_history--{domain}.py')],cwd=root,env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'},check=True,capture_output=True)
                with (root/'processed_source'/f'medical_history--{domain}.csv').open() as stream:
                    rows=list(csv.DictReader(stream))
                self.assertEqual(len(rows),6)
                self.assertEqual([r[datefield] for r in rows],['2016-01-01']*2+['1900-01-01']*4)
                for r in rows:
                    provenance=r[domain.split('_')[0]+'_source_value']
                    self.assertIn('Days since intake',provenance)
                    self.assertIn('+date_status:',provenance)
                    if domain=='drug_exposure':
                        self.assertEqual(r['drug_exposure_end_date'],r[datefield])
                        self.assertEqual(r['verbatim_end_date'],r[datefield])
                audit=json.loads((root/'logs'/f'medical-history-dates-{domain}.json').read_text())
                self.assertIsNone(audit['profile_sha256'])
                self.assertEqual(audit['medical_history_units'], 'relative-days')
                self.assertEqual(audit['emitted_status_counts'],{'valid':2,'missing':2,'invalid':2})
                self.assertEqual([r['raw_value'] for r in audit['records']],['0','NA','-736918'])

if __name__=='__main__':unittest.main()
