# Reproduce a pinned ETL revision

Run `run.py` with a specific Git revision, original-format clinical input directory,
auxiliary source directory, and a fresh output directory outside this checkout.
Use Python 3.12.14 with the versions in `requirements.lock.txt` to reproduce the
recorded baseline environment. A standard virtual environment can install that
file with `python -m pip install -r tools/baseline/requirements.lock.txt`.

```sh
python tools/baseline/run.py \
  --ref COMMIT_SHA \
  --clinical /path/to/original-format-clinical-inputs \
  --auxiliary /path/to/auxiliary-source-tables \
  --output /path/to/private-runs/new-run
```

For current V1.1 development revisions, also pass `--identity-policy /path/to/private-policy`, containing the pinned `person_id_registry.csv` and `exclusions.json`. Historical revisions before identity-policy support do not require this argument. See [identity handling](../../docs/identity-handling.md); these policy inputs are copied and hashed alongside the clinical and auxiliary inputs.

The runner exports committed code without working-tree edits, copies inputs,
and invokes the original pipeline in that separate directory. It records the
actual environment, input hashes, output schemas/counts/hashes, log-level counts,
return code, missing expected outputs and source/code integrity checks.
The original pipeline may rename staged files and clear generated directories;
never execute it against source snapshots. Existing run directories are refused.

Run folders contain participant data and potentially sensitive logs. Keep them
outside Git and do not attach them to public issues or pull requests. Only
synthetic test fixtures and generic tooling belong in this repository.

An exit code of zero is not a clinical or OMOP validation pass. Review errors,
missing outputs, source retention, identity linkage and differences from the
released dataset. Preserve failed runs as evidence. Record minimal execution
repairs in distinct commits and distinguish their runs from untouched baseline
runs. Unresolved source, mapping and historical-release questions remain open.

Run the synthetic checks with `PYTHONDONTWRITEBYTECODE=1 python -m unittest
discover -s tests -v`. These include the narrow gene-row execution repair and
the comparator's identity and multiplicity behavior.

`compare.py --run RUN --released RELEASE_DIRECTORY --repeat SECOND_RUN
--output REPORT.json` writes aggregate comparison and reproducibility results.
Generated row IDs are excluded from content matching; person IDs are translated
through source identity and foreign-key membership is reported separately.
Only common columns are compared, with differences in columns listed explicitly.
The private report is diagnostic, not a conformance certificate.
