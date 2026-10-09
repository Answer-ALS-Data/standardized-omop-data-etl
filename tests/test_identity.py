"""Synthetic identity acceptance cases; no clinical data in fixtures."""
import importlib.util
from pathlib import Path
import sys
import json
import tempfile
import unittest

SRC = Path(__file__).resolve().parents[1]/'src/second_scripts'
sys.path.insert(0, str(SRC))
from identity import IdentityError, final_id, integer_id, source_id, read_table, write_table
import person_id_map
import transform_ids


class IdentityTests(unittest.TestCase):
    def test_integral_representations(self):
        self.assertEqual({final_id(v) for v in [7,7.0,'7','7.0','007',' 7.00 ']}, {'110000007'})

    def test_invalid_ids_rejected(self):
        for value in ['',None,'NA','nan',float('nan'),'abc7','7_2','-7','0','7.5','7e0','inf']:
            with self.subTest(value=value), self.assertRaises(IdentityError):
                integer_id(value)

    def test_opaque_source_identity(self):
        self.assertEqual(source_id(' SYN_001 '), 'SYN_001')
        self.assertNotEqual(source_id('001'), source_id('1'))
        self.assertNotEqual(source_id('7.0'), source_id('7'))

    def test_no_truncation(self):
        self.assertEqual(final_id('9999999'), '119999999')
        with self.assertRaises(IdentityError):final_id('10000000')

    def fixture(self, root, people=('SYN_A','SYN_B'), events=('SYN_B','SYN_A')):
        write_table(root/'person.csv',['person_id','year_of_birth'],
                    [{'person_id':p,'year_of_birth':'1970'} for p in people])
        write_table(root/'death.csv',['death_id','person_id','death_date','cause_source_value'],
                    [{'death_id':str(i),'person_id':p,'death_date':'2020-01-01','cause_source_value':'synthetic'} for i,p in enumerate(events,1)])

    def test_roundtrip_intended_links_and_unchanged_nonids(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); self.fixture(root)
            report=person_id_map.remap(root)
            self.assertEqual(report['status'],'passed')
            transform_ids.main(root,root/'final')
            rows=read_table(root/'final/death.csv')[1]
            self.assertEqual([r['person_id'] for r in rows],['110000002','110000001'])
            self.assertEqual([r['death_id'] for r in rows],['110000001','110000002'])
            self.assertTrue(all(r['cause_source_value']=='synthetic' and r['death_date']=='2020-01-01' for r in rows))
            self.assertEqual(read_table(root/'final/person.csv')[1][0]['person_id'],'110000001')

    def test_orphan_blocks_before_csv_changes(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); self.fixture(root,events=('SYN_B','SYN_ORPHAN'))
            before={p.name:p.read_bytes() for p in root.glob('*.csv')}
            with self.assertRaises(IdentityError):person_id_map.remap(root)
            self.assertEqual(before,{p.name:p.read_bytes() for p in root.glob('*.csv')})
            self.assertIn('Source identity absent from PERSON',(root/'identity-validation.json').read_text())

    def test_invalid_person_link_blocks(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); self.fixture(root,events=('nan',))
            with self.assertRaises(IdentityError):person_id_map.remap(root)

    def test_duplicate_people_normalized_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); self.fixture(root,people=('SYN_A',' SYN_A '))
            with self.assertRaises(IdentityError):person_id_map.remap(root)

    def test_row_order_identity_not_allocation(self):
        # New allocations are sorted by source ID, independent of PERSON row order.
        for people in [('SYN_A','SYN_B'),('SYN_B','SYN_A')]:
            with self.subTest(people=people), tempfile.TemporaryDirectory() as d:
                root=Path(d);self.fixture(root,people=people)
                person_id_map.remap(root)
                reverse={r['person_id']:r['Participant_ID'] for r in read_table(root/'person_id_mapping.csv')[1]}
                self.assertEqual([reverse[r['person_id']] for r in read_table(root/'death.csv')[1]],['SYN_B','SYN_A'])
                self.assertEqual(reverse, {'1':'SYN_A','2':'SYN_B'})

    def test_invalid_late_table_does_not_publish_earlier(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); self.fixture(root,events=('NaN',))
            with self.assertRaises(IdentityError):transform_ids.main(root,root/'final')
            self.assertFalse((root/'final').exists())

    def test_multiplicity_retained(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);self.fixture(root,events=('SYN_B','SYN_B'))
            person_id_map.remap(root)
            self.assertEqual(len(read_table(root/'death.csv')[1]),2)


    def pinned(self, root):
        path=root/'registry.csv'
        write_table(path,['Participant_ID','person_id'],[
            {'Participant_ID':'SYN_B','person_id':'110000042'},
            {'Participant_ID':'ABSENT_HISTORICAL','person_id':'110000099'}])
        return path

    def rules(self, root, expected=1):
        path=root/'rules.json'
        path.write_text(json.dumps({'version':1,'exclusions':[{
            'Participant_ID':'SYN_ORPHAN','table':'death.csv','expected_rows':expected,
            'approved_on':'2026-10-09','reason':'Synthetic approved missing PERSON exception'}]}))
        return path

    def test_registry_preserves_and_reserves_ids(self):
        for people in [('SYN_A','SYN_B'),('SYN_B','SYN_A')]:
            with self.subTest(people=people), tempfile.TemporaryDirectory() as d:
                root=Path(d);self.fixture(root,people=people)
                person_id_map.remap(root,self.pinned(root))
                result={r['Participant_ID']:r['person_id'] for r in read_table(root/'person_id_registry.csv')[1]}
                self.assertEqual(result,{'SYN_A':'110000100','SYN_B':'110000042','ABSENT_HISTORICAL':'110000099'})
                # A later build reuses the extended registry, not the original seed.
                self.fixture(root,people=people)
                person_id_map.remap(root,root/'person_id_registry.csv')
                self.assertEqual(result,{r['Participant_ID']:r['person_id'] for r in read_table(root/'person_id_registry.csv')[1]})

    def test_approved_exclusion_is_exact_and_counted(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);self.fixture(root,events=('SYN_B','SYN_ORPHAN'))
            report=person_id_map.remap(root,self.pinned(root),self.rules(root))
            self.assertEqual(report['clinical_rows_dropped'],1)
            self.assertEqual(len(read_table(root/'death.csv')[1]),1)
            self.assertEqual(report['reviewed_exclusions'][0]['record_ordinal'],2)

    def test_exclusion_does_not_cover_other_orphans(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);self.fixture(root,events=('SYN_ORPHAN','ANOTHER_ORPHAN'))
            before=(root/'death.csv').read_bytes()
            with self.assertRaises(IdentityError):person_id_map.remap(root,self.pinned(root),self.rules(root))
            self.assertEqual(before,(root/'death.csv').read_bytes())

    def test_stale_exclusion_count_blocks(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);self.fixture(root,events=('SYN_ORPHAN','SYN_ORPHAN'))
            with self.assertRaises(IdentityError):person_id_map.remap(root,self.pinned(root),self.rules(root))

    def test_recovered_demographics_requires_review(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);self.fixture(root,people=('SYN_ORPHAN',),events=('SYN_ORPHAN',))
            with self.assertRaises(IdentityError):person_id_map.remap(root,self.pinned(root),self.rules(root))

    def test_registry_collision_blocks(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);self.fixture(root)
            path=self.pinned(root)
            write_table(path,['Participant_ID','person_id'],[
                {'Participant_ID':'SYN_A','person_id':'110000042'},
                {'Participant_ID':'SYN_B','person_id':'110000042'}])
            with self.assertRaises(IdentityError):person_id_map.remap(root,path)


if __name__=='__main__':unittest.main()
