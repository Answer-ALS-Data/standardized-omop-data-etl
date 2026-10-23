import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


class BloodPressurePositions(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cwd = Path.cwd()
        os.chdir(self.tmp.name)
        Path('logs').mkdir()
        sys.path.insert(0, str(ROOT / 'src/table_scripts'))
        spec = importlib.util.spec_from_file_location(
            'bp_vitals', ROOT / 'src/table_scripts/vital_signs--measurement.py')
        self.mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.mod)

    def tearDown(self):
        sys.path.pop(0)
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def transform(self, position, day=0):
        row = dict.fromkeys(['temp', 'hr', 'rr', 'weight', 'height', 'bmi'], float('nan'))
        row.update(Participant_ID='SYNTHETIC', vsdt=day, bpsys=129,
                   bpdias=85, bppos=position)
        return self.mod.vital_signs_to_measurement(pd.DataFrame([row]), '2016-01-01')

    def test_explicit_positions_preserve_numeric_results(self):
        cases = [(1, 'Standing', [3035856, 3019962]),
                 (2, 'Sitting', [3018586, 3034703]),
                 (3, 'Supine', [3009395, 3013940])]
        for position, label, concepts in cases:
            with self.subTest(position=position):
                out = self.transform(position)
                self.assertEqual(list(out.measurement_concept_id), concepts)
                self.assertEqual(list(out.value_as_number), [129, 85])
                self.assertTrue(out.value_as_concept_id.isna().all())
                self.assertEqual(list(out.unit_concept_id), [8876, 8876])
                self.assertEqual(list(out.measurement_date), ['2016-01-01'] * 2)
                self.assertTrue(out.value_source_value.str.contains(label).all())

    def test_missing_and_unrecognized_positions_keep_generic_measurements(self):
        for position in [None, float('nan'), '', 0, 9]:
            with self.subTest(position=position):
                out = self.transform(position)
                self.assertEqual(list(out.measurement_concept_id), [4152194, 4154790])
                self.assertTrue(out.value_as_concept_id.isna().all())
                self.assertEqual(list(out.value_as_number), [129, 85])

    def test_missing_date_keeps_existing_sentinel(self):
        out = self.transform(2, float('nan'))
        self.assertEqual(list(out.measurement_date), ['1900-01-01'] * 2)
