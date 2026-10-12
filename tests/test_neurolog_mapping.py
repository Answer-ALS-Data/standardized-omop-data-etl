"""NEUROLOG whitespace repair retains source evidence and mapping equivalence."""
import csv
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


def write(path, rows):
    with path.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


class NeurologMapping(unittest.TestCase):
    def run_fixture(self, root, mappings):
        for name in ['source_tables/usagi', 'logs', 'processed_source']:
            (root / name).mkdir(parents=True)
        write(root / 'source_tables/neurolog.csv', [
            dict(Participant_ID='SYNTHETIC', Visit_Date='0', date1='0', other='', hidden2=v)
            for v in ['Progressive Spinal Muscular Atrophy', 'Progressive Spinal Muscular Atrophy ', '', 'unknown term']
        ])
        write(root / 'source_tables/usagi/neurolog_mapping_v3.csv', mappings)
        return subprocess.run([sys.executable, str(ROOT / 'src/table_scripts/neurolog--condition_occurrence.py')],
                              cwd=root, env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'}, capture_output=True, text=True)

    def test_trimmed_match_keeps_equivalence_and_raw_provenance(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            result = self.run_fixture(root, [dict(sourceName=' Progressive Spinal Muscular Atrophy', conceptId='376679', conceptName='Progressive muscular atrophy', equivalence='EQUIVALENT')])
            self.assertEqual(result.returncode, 0, result.stderr)
            with (root / 'processed_source/neurolog--condition_occurrence.csv').open() as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), 2)
            for r in rows:
                self.assertEqual(r['condition_concept_id'], '376679')
                self.assertEqual(r['condition_start_date'], '2016-01-01')
                self.assertIn(': EQUIVALENT', r['condition_source_value'])
            self.assertIn('Atrophy  |', rows[1]['condition_source_value'])

    def test_colliding_labels_fail_without_output(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            result = self.run_fixture(root, [dict(sourceName=v, conceptId=str(i), conceptName='Synthetic', equivalence='EQUAL')
                                             for i, v in enumerate(['Progressive Spinal Muscular Atrophy', 'Progressive Spinal Muscular Atrophy '], 1)])
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('Ambiguous NEUROLOG', result.stderr)
            self.assertFalse((root / 'processed_source/neurolog--condition_occurrence.csv').exists())


if __name__ == '__main__':
    unittest.main()
