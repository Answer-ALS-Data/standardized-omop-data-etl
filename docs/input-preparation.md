# Prepare clinical inputs

`src/prepare_inputs.py` accepts original filenames such as `Medical_History.csv`
and NeuroBank filenames such as `v_NB_IATI_Medical_History.csv`, including case
variations. It writes canonical lowercase filenames into a fresh `clinical/`
directory and a `preparation.json` manifest beside it. Original files are never
renamed or edited.

```sh
python src/prepare_inputs.py \
  --source /path/to/clinical-export \
  --crosswalk /path/to/pinned-subjects.csv \
  --medical-history-units relative-days \
  --output /path/to/private-prepared-inputs
```

The September V1.1 source uses `relative-days`, which is also the current ETL
default. This preparation argument records the supplied units; it does **not**
convert dates. For older year-based inputs, select `years` here and supply the
resulting preparation.json through the runner's `--medical-history-profile`
option. See [medical-history dates](medical-history-dates.md).

The crosswalk must contain `SubjectUID` and `Participant_ID`. It is optional when
every clinical file already supplies Participant_ID; when supplied, it also checks
existing IDs. SubjectUIDs are opaque strings: only surrounding whitespace is
ignored for matching, with no numeric coercion or removal of leading zeroes.
Missing/ambiguous links and reverse identity collisions fail preparation. Repeated
identical crosswalk rows do not multiply clinical rows. Identifiers, source
values, row order and duplicates are otherwise retained unchanged.

Known header aliases are `Visit Date`, `Visit Name`, `Form Name`, `Child Name`
and `Participant ID`, mapped to their underscore forms. Two files resolving to
the same canonical name, or two columns resolving to the same header, are rejected.
The preparer checks required core fields and uses the legacy pandas parsing
conventions, including nullable integers where existing readers specify them.
This is not complete dictionary, clinical, concept or OMOP validation. Additional
columns are retained but acquire no new mapping. Clinical dictionary CSVs beginning
`__Clinical_Dictionary_` are carried through as ancillary files without ID joins.

Each manifest records source and prepared hashes, aliases, row counts, crosswalk
provenance and the preparer's hash. Original cell values can be traced using the
source CSV record ordinal after the header, preserved one-to-one in the prepared
file; quoted multiline fields do not change record ordinal. When no header or ID
change is needed, the source file is copied byte-for-byte. Validation completes
before the new output directory is published. Existing output directories are
refused rather than overwritten.

For an old year-based source, feed the prepared `clinical/` directory into the
[isolated runner](../tools/baseline/README.md) as its `--clinical` argument. Supply
the same separately pinned auxiliary mapping/reference files as the baseline.
The main legacy pipeline still reads canonical `source_tables` filenames;
preparation is an explicit preceding command. Keep all prepared inputs,
manifests and ETL outputs outside the public code repository.

Run the synthetic checks with:

```sh
PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -s tests -v
```
