import csv
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('birth_eligibility', Path(__file__).resolve().parents[1] / 'src/second_scripts/exclude_missing_birth_year.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class BirthEligibility(unittest.TestCase):
    def test_excludes_linked_records_preserves_others_and_audits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'person.csv').write_text('person_id,year_of_birth\n1,\n2,1970\n3,NA\n')
            (root / 'observation.csv').write_text('observation_id,person_id,observation_date\n10,1,1900-01-01\n11,2,1900-01-01\n12,3,2016-01-01\n')
            (root / 'death.csv').write_text('person_id,death_date\n1,1900-01-01\n')
            (root / 'concept.csv').write_text('concept_id\n123\n')
            report = module.exclude_missing_birth_year(root, root / 'audit.json')
            self.assertEqual(len(report['excluded_people']), 2)
            self.assertEqual(len(report['excluded_rows_by_table']['observation']), 2)
            with (root / 'observation.csv').open() as f:
                self.assertEqual(list(csv.DictReader(f)), [{'observation_id': '11', 'person_id': '2', 'observation_date': '1900-01-01'}])
            self.assertEqual((root / 'concept.csv').read_text(), 'concept_id\n123\n')
            with (root / 'death.csv').open() as f:
                self.assertEqual(list(csv.DictReader(f)), [])
            second = module.exclude_missing_birth_year(root, root / 'second-audit.json')
            self.assertEqual(second['excluded_people'], [])
