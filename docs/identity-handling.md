# Identity handling during V1.1 development

Source `Participant_ID` and `SubjectUID` values are opaque strings. Person lookup is exact after surrounding whitespace trimming, preserving case and leading zeroes. Numeric validation applies only to generated OMOP IDs.

The pipeline requires two private, versioned policy files in `source_tables/identity/`. Keep actual participant identities and clinical exceptions outside the code repository:

- `person_id_registry.csv`: exactly `Participant_ID,person_id`, with the latter a published nine-digit `11`-prefixed ID. Entries must be unique on both columns. All IDs are reserved, even for people absent from the current build. Existing participants retain their IDs; new participants, sorted by source ID, receive IDs above the reserved maximum. Persist the resulting `combined_omop/person_id_registry.csv` for subsequent releases so later additions cannot renumber existing people.
- `exclusions.json`: `{"version":1,"exclusions":[]}` or narrowly reviewed rules. Each rule requires `table` (e.g. `death.csv`), `Participant_ID`, positive integer `expected_rows`, nonempty `reason`, and `approved_on`. Only that table/source identity is excluded. Any count change or recovered PERSON record blocks the build for renewed review. Unlisted unmatched source identities remain errors.

Supply both through the isolated runner's `--identity-policy DIRECTORY` option; their hashes enter its input manifest. The approved source-specific exclusion and registry seed are retained privately in the project scope, not hardcoded in Python.

`person_id_map.py` validates PERSON uniqueness and all available event-table links before changing CSVs. Its `identity-validation.json` records policy and input-table hashes, one-based data-record ordinals for reviewed exclusions, row counts and unresolved links. A blocked validation changes no CSVs. Invalid PERSON/schema/policy input can fail before that report. Original source tables are never edited. On success it saves the numeric suffix map for the remaining pipeline and the complete nine-digit registry for future runs.

`transform_ids.py` accepts positive integer IDs and integral decimal representations such as `7.0`. It formats the existing `11` prefix plus seven-digit namespace exactly once. Missing, malformed, fractional, negative, zero and oversized numeric values raise instead of becoming plausible IDs or losing digits. All available tables are checked before transformed outputs are written.

V1.1 exports no visit tables, so final `visit_occurrence_id` and `visit_detail_id` values are blank. Raw visit tokens remain in staged data. After ID conversion, `exclude_missing_birth_year.py` removes people without a birth year and their linked records, with a private audit; the event-date sentinel does not supply a birth year. A successful build is not overall CDM validation.

Run tests with `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python3 -m unittest discover -s tests`. Identity cases include source-string preservation, numeric forms, rejection boundaries, orphan failure without mutation, registry collision/reservation, row-order independence, subsequent-registry reuse, exact exclusion accounting and stale-policy detection.
