"""Check that diagnostic comparisons preserve identity and row multiplicity."""
import importlib.util
from pathlib import Path
import unittest

path = Path(__file__).resolve().parents[1]/'tools/baseline/compare.py'
spec = importlib.util.spec_from_file_location('baseline_compare', path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def fixture(person_id, copies):
    person = {'person_id': person_id, 'person_source_value':
              'demographics+Participant_ID (participant identifier): SYNTHETIC_A'}
    event = {'person_id': person_id, 'measurement_id': '99',
             'measurement_concept_id': '123', 'value_as_number': '4.0'}
    return {'person.csv': (list(person), [person]),
            'measurement.csv': (list(event), [event.copy() for _ in range(copies)])}


class BaselineComparison(unittest.TestCase):
    def test_reassigned_person_ids_and_duplicate_counts(self):
        result = module.compare(fixture('1', 2), fixture('77.0', 1))['measurement.csv']
        self.assertEqual(result['common_row_matches'], 1)
        self.assertEqual(result['left_only_rows'], 1)
        self.assertEqual(result['right_only_rows'], 0)

    def test_bad_foreign_key_cannot_match_valid_person(self):
        left = fixture('1', 1)
        left['measurement.csv'][1][0]['person_id'] = '9'
        self.assertEqual(module.diagnostics(left)['measurement.csv']['person_fk_missing_rows'], 1)
        result = module.compare(left, fixture('1', 1))['measurement.csv']
        self.assertEqual(result['common_row_matches'], 0)


if __name__ == '__main__':
    unittest.main()
