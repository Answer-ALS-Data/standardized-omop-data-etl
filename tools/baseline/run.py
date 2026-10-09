"""Run an untouched Git revision with copied inputs; retain private audit artifacts.

This characterizes historical behavior. It does not certify OMOP conformance.
Use a new output directory outside this checkout for each run.
"""
import argparse
import csv
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tarfile
import time
from datetime import datetime, timezone

REPO = Path(__file__).resolve().parents[2]
AUXILIARY = (
    'usagi/medical_history_conditions_v2.csv',
    'usagi/neurolog_mapping_v3.csv',
    'usagi/medications_v2.csv',
    'usagi/Mortality OMOP Mapping.csv',
    'omop_tables/concept.csv', 'omop_tables/care_site.csv',
    'other/omic_inferred_sex_if_different.csv',
)
REQUIRED_CLINICAL = (
    'aalsdxfx.csv', 'aalshxfx.csv', 'als_gene_mutations.csv', 'alsfrs_r.csv',
    'answer_als_medications_log.csv', 'auxiliary_chemistry_labs.csv',
    'auxiliary_chemistry.csv', 'demographics.csv', 'subjects.csv',
    'environmental_questionnaire.csv', 'family_history_log.csv',
    'medical_history.csv', 'neurolog.csv', 'vital_signs.csv',
)
EXPECTED_FINAL = (
    'person.csv', 'condition_occurrence.csv', 'observation.csv', 'measurement.csv',
    'drug_exposure.csv', 'procedure_occurrence.csv', 'device_exposure.csv',
    'death.csv', 'observation_period.csv', 'concept.csv', 'care_site.csv',
)


def sha(path):
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def csv_profile(path):
    with path.open(encoding='utf-8-sig', newline='') as f:
        reader = csv.reader(f)
        header = next(reader, [])
        count = bad = 0
        for row in reader:
            count += 1
            bad += len(row) != len(header)
    return dict(rows=count, columns=header, malformed_width_rows=bad,
                bytes=path.stat().st_size, sha256=sha(path))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ref', required=True)
    parser.add_argument('--clinical', required=True, type=Path)
    parser.add_argument('--auxiliary', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    clinical, auxiliary, output = (p.resolve() for p in
                                   (args.clinical, args.auxiliary, args.output))
    if output.exists():
        parser.error('Output already exists; use a fresh directory')
    if any(output.is_relative_to(p) or p.is_relative_to(output)
           for p in (clinical, auxiliary, REPO)):
        parser.error('Output must not overlap the checkout or input directories')
    commit = subprocess.check_output(
        ['git', '-C', str(REPO), 'rev-parse', '--verify', args.ref + '^{commit}'],
        text=True).strip()
    clinical_files = sorted(clinical.glob('*.csv'))
    names = [p.name.lower() for p in clinical_files]
    if len(set(names)) != len(names):
        parser.error('Clinical filenames collide after baseline lowercasing')
    missing = sorted(set(REQUIRED_CLINICAL) - set(names))
    if missing:
        parser.error('Missing original-format clinical tables: ' + ', '.join(missing))
    sources = [(p, Path(p.name)) for p in clinical_files]
    sources += [(auxiliary / n, Path(n)) for n in AUXILIARY]
    for p, _ in sources:
        if not p.is_file():
            parser.error('Missing input: ' + str(p))
    versions = {n: importlib.metadata.version(n) for n in
                ('pandas', 'numpy', 'python-dateutil', 'pytz', 'tzdata', 'six')}
    report = dict(code_commit=commit, harness_sha256=sha(Path(__file__)),
                  started_utc=datetime.now(timezone.utc).isoformat(),
                  python_executable=sys.executable, python_version=sys.version,
                  platform=platform.platform(), packages=versions,
                  timezone='UTC', inputs=[],
                  interpretation='Unmodified baseline behavior, not validated OMOP output')
    output.mkdir(parents=True)
    work = output / 'work'
    work.mkdir()
    archive = output / 'code.tar'
    with archive.open('wb') as f:
        subprocess.run(['git', '-C', str(REPO), 'archive', commit], stdout=f, check=True)
    with tarfile.open(archive) as tar:
        tar.extractall(work, filter='data')
    code_hashes = {str(p.relative_to(work)): sha(p) for p in work.rglob('*') if p.is_file()}
    for origin, relative in sources:
        target = work / 'source_tables' / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(origin, target)
        digest = sha(origin)
        assert sha(target) == digest
        report['inputs'].append(dict(origin=str(origin), staged=str(relative), sha256=digest))
    save(output / 'manifest.json', report)
    env = os.environ.copy()
    env.update(PATH=str(Path(sys.executable).parent) + os.pathsep + env.get('PATH', ''),
               TZ='UTC', PYTHONDONTWRITEBYTECODE='1', PYTHONHASHSEED='0', PYTHONUNBUFFERED='1')
    env.pop('PYTHONPATH', None)
    before = time.monotonic()
    with (output / 'pipeline.log').open('w') as log:
        completed = subprocess.run([sys.executable, 'src/pipeline_process_subtables_to_final.py'],
                                   cwd=work, env=env, stdout=log, stderr=subprocess.STDOUT)
    report.update(exit_code=completed.returncode, elapsed_seconds=round(time.monotonic()-before, 3),
                  finished_utc=datetime.now(timezone.utc).isoformat())
    report['original_input_hashes_unchanged'] = all(sha(Path(i['origin'])) == i['sha256']
                                                   for i in report['inputs'])
    report['staged_input_bytes_unchanged'] = all(
        sha(work/'source_tables'/Path(i['staged']).parent/
            (Path(i['staged']).name.lower() if Path(i['staged']).parent == Path('.')
             else Path(i['staged']).name)) == i['sha256'] for i in report['inputs'])
    report['code_files_unchanged'] = all((work/p).is_file() and sha(work/p)==h
                                         for p,h in code_hashes.items())
    report['stages'] = {stage: {p.name: csv_profile(p) for p in sorted((work/stage).glob('*.csv'))}
                        for stage in ('processed_source', 'combined_omop', 'final_omop', 'redundant')}
    report['missing_expected_final_files'] = sorted(set(EXPECTED_FINAL)-set(report['stages']['final_omop']))
    report['log_level_counts'] = {}
    for p in sorted((work/'logs').glob('*.log')):
        counts = {level: 0 for level in ('ERROR', 'WARNING')}
        for line in p.read_text(errors='replace').splitlines():
            for level in counts:
                counts[level] += f' - {level} - ' in line
        report['log_level_counts'][p.name] = counts
    save(output/'manifest.json', report)
    print(json.dumps({k: report[k] for k in ('code_commit','exit_code','elapsed_seconds',
          'original_input_hashes_unchanged','staged_input_bytes_unchanged',
          'code_files_unchanged','missing_expected_final_files')}, indent=2))
    print('Final row counts:', json.dumps({n:v['rows'] for n,v in report['stages']['final_omop'].items()}))
    return completed.returncode


if __name__ == '__main__':
    raise SystemExit(main())
