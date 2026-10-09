"""Synthetic acceptance cases for rows without any mapped gene responses."""
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest

import pandas as pd

SOURCE = Path(__file__).resolve().parents[1] / 'src/table_scripts'


class GeneInputRetention(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        previous = Path.cwd()
        try:
            os.chdir(cls.temp.name)
            Path('logs').mkdir()
            sys.path.insert(0, str(SOURCE))
            spec = importlib.util.spec_from_file_location(
                'gene_etl', SOURCE/'als_gene_mutations--measurement.py')
            cls.module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(cls.module)
        finally:
            os.chdir(previous)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def transform(self, rows):
        return self.module.als_gene_mutations_to_measurement(pd.DataFrame(rows), '2016-01-01')

    def test_empty_response_row_needs_no_date(self):
        result = self.transform([{'Participant_ID': 'SYNTHETIC_EMPTY',
                                  'Visit_Date': pd.NA, 'sod1': pd.NA}])
        self.assertTrue(result.empty)
        self.assertIn('measurement_date', result.columns)

    def test_empty_row_does_not_change_mapped_results(self):
        mapped = {'Participant_ID': 'SYNTHETIC_MAPPED', 'Visit_Date': 0,
                  'sod1': 1, 'c9orf72': 2}
        empty = {'Participant_ID': 'SYNTHETIC_EMPTY', 'Visit_Date': pd.NA,
                 'sod1': pd.NA, 'c9orf72': pd.NA}
        expected = self.transform([mapped])
        actual = self.transform([empty, mapped])
        pd.testing.assert_frame_equal(expected, actual)
        self.assertEqual(set(actual['measurement_date']), {'2016-01-01'})
        self.assertEqual(set(actual['value_as_concept_id']), {9191, 9189})
        self.assertEqual(len(actual), 2)

    def test_unmapped_response_codes_do_not_acquire_dates(self):
        self.assertTrue(self.transform([{'Participant_ID': 'SYNTHETIC_UNMAPPED',
                                        'Visit_Date': pd.NA, 'sod1': 3}]).empty)

    def test_mapped_response_with_missing_date_is_not_silently_dropped(self):
        # Missing-date policy is intentionally unchanged by this narrow repair.
        with self.assertRaises(AttributeError):
            self.transform([{'Participant_ID': 'SYNTHETIC_MISSING_DATE',
                             'Visit_Date': pd.NA, 'sod1': 1}])


if __name__ == '__main__':
    unittest.main()
