"""Synthetic compatibility and failure-boundary tests; no participant data."""
import csv
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('prepare_inputs', Path(__file__).resolve().parents[1]/'src/prepare_inputs.py')
prep = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prep)


def write(path, headers, rows):
    with path.open('w', newline='') as f:
        w=csv.writer(f); w.writerow(headers); w.writerows(rows)


class InputPreparation(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.source=self.root/'source'; self.source.mkdir()
        for name, cols in prep.REQUIRED.items():
            headers=['SubjectUID','Participant_ID']+cols
            values=['0001','SYNTHETIC_A']+['0' for _ in cols]
            write(self.source/name, headers, [values])
        self.crosswalk=self.root/'crosswalk.csv'
        write(self.crosswalk,['SubjectUID','Participant_ID'],[['0001','SYNTHETIC_A']])

    def run_prepare(self, source=None, output='prepared', crosswalk=True):
        return prep.prepare(source or self.source,self.root/output,
                            self.crosswalk if crosswalk else None,'years')

    def rewrite(self,name,edit):
        path=self.source/name
        with path.open(newline='') as f: data=list(csv.reader(f))
        edit(data)
        write(path,data[0],data[1:])

    def test_original_files_preserve_bytes(self):
        report=self.run_prepare(crosswalk=False)
        self.assertTrue(all(f['byte_preserved'] for f in report['files']))
        for path in self.source.iterdir():
            self.assertEqual(path.read_bytes(),(self.root/'prepared/clinical'/path.name).read_bytes())

    def test_prefixed_headers_restore_identity_without_changing_cells(self):
        alternate=self.root/'alternate'; alternate.mkdir()
        expected={}
        for path in self.source.iterdir():
            with path.open(newline='') as f: rows=list(csv.reader(f))
            expected[path.name]=rows
            pid=rows[0].index('Participant_ID')
            stripped=[[v for i,v in enumerate(row) if i!=pid] for row in rows]
            reverse={v:k for k,v in prep.ALIASES.items()}
            stripped[0]=[reverse.get(c,c) for c in stripped[0]]
            write(alternate/('v_NB_IATI_'+path.name.upper()),stripped[0],stripped[1:])
        report=self.run_prepare(alternate)
        self.assertEqual(report['linked_unique_subjects'],1)
        self.assertTrue(all(f['participant_id_added'] for f in report['files']))
        for name,rows in expected.items():
            with (self.root/'prepared/clinical'/name).open(newline='') as f: actual=list(csv.DictReader(f))
            self.assertEqual(actual,[dict(zip(rows[0],r)) for r in rows[1:]])

    def test_duplicates_missing_markers_and_text_are_preserved(self):
        def edit(rows):
            rows[0].append('free_text'); rows[1].append('quoted, text\nwith café')
            rows[1][rows[0].index('medhxyr')]='NA'
            rows.append(rows[1].copy())
            index=rows[0].index('Participant_ID')
            for row in rows: row.pop(index)
        self.rewrite('medical_history.csv',edit)
        self.run_prepare()
        with (self.root/'prepared/clinical/medical_history.csv').open(newline='') as f: rows=list(csv.DictReader(f))
        self.assertEqual(len(rows),2); self.assertEqual(rows[0],rows[1])
        self.assertEqual(rows[0]['medhxyr'],'NA')
        self.assertEqual(rows[0]['free_text'],'quoted, text\nwith café')

    def test_filename_collision(self):
        (self.source/'v_NB_IATI_subjects.csv').write_bytes((self.source/'subjects.csv').read_bytes())
        with self.assertRaisesRegex(ValueError,'Multiple input'): self.run_prepare()
        self.assertFalse((self.root/'prepared').exists())

    def test_header_alias_collision(self):
        self.rewrite('aalsdxfx.csv',lambda rows:(rows[0].append('Visit Date'),rows[1].append('0')))
        with self.assertRaisesRegex(ValueError,'colliding'):self.run_prepare()

    def test_missing_required_table_and_column(self):
        (self.source/'subjects.csv').unlink()
        with self.assertRaisesRegex(ValueError,'Missing required tables'):self.run_prepare()
        write(self.source/'subjects.csv',['SubjectUID','Participant_ID'],[['0001','SYNTHETIC_A']])
        with self.assertRaisesRegex(ValueError,'missing required columns'):self.run_prepare()

    def test_crosswalk_conflict_and_reverse_collision(self):
        for rows in [[['0001','SYNTHETIC_A'],['0001','SYNTHETIC_B']],
                     [['0001','SYNTHETIC_A'],['0002','SYNTHETIC_A']]]:
            write(self.crosswalk,['SubjectUID','Participant_ID'],rows)
            with self.assertRaises(ValueError):self.run_prepare()

    def test_supplied_id_conflict(self):
        self.rewrite('subjects.csv',lambda rows:rows[1].__setitem__(1,'SYNTHETIC_B'))
        with self.assertRaisesRegex(ValueError,'conflicts with crosswalk'):self.run_prepare()

    def test_repeated_identical_crosswalk_does_not_multiply_rows(self):
        write(self.crosswalk,['SubjectUID','Participant_ID'],[['0001','SYNTHETIC_A']]*2)
        report=self.run_prepare()
        self.assertEqual(report['crosswalk']['unique_subjects'],1)
        self.assertTrue(all(f['rows']==1 for f in report['files']))

    def test_missing_crosswalk_and_malformed_rows(self):
        self.rewrite('subjects.csv',lambda rows:[row.pop(1) for row in rows])
        with self.assertRaisesRegex(ValueError,'pinned crosswalk'):self.run_prepare(crosswalk=False)
        self.rewrite('subjects.csv',lambda rows:rows[1].append('extra'))
        with self.assertRaisesRegex(ValueError,'row width'):self.run_prepare()

    def test_unmatched_uid_and_opaque_leading_zeroes(self):
        self.rewrite('subjects.csv',lambda rows:rows[1].__setitem__(0,'1'))
        with self.assertRaisesRegex(ValueError,'absent from crosswalk'):self.run_prepare()

    def test_blank_identity(self):
        self.rewrite('subjects.csv',lambda rows:rows[1].__setitem__(1,''))
        with self.assertRaisesRegex(ValueError,'Blank identifier'):self.run_prepare()

    def test_numeric_fraction_rejected_by_legacy_integer_reader(self):
        self.rewrite('alsfrs_r.csv',lambda rows:rows[1].__setitem__(rows[0].index('alsfrs1'),'1.5'))
        with self.assertRaisesRegex(ValueError,'legacy CSV parsing failed'):self.run_prepare()
        self.assertFalse((self.root/'prepared').exists())

    def test_refuses_overwrite_and_source_overlap(self):
        self.run_prepare()
        with self.assertRaisesRegex(ValueError,'fresh output'):self.run_prepare()
        with self.assertRaisesRegex(ValueError,'fresh output'):
            prep.prepare(self.source,self.source/'nested',medical_history_units='years')

    def test_no_inference_of_date_units(self):
        with self.assertRaisesRegex(ValueError,'Explicit medical-history'):
            prep.prepare(self.source,self.root/'prepared')


if __name__=='__main__':unittest.main()
