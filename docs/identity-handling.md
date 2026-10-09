# Identity handling during V1.1 development

Source `Participant_ID` and `SubjectUID` values are opaque strings. The person remapper uses exact Participant_ID lookup after surrounding whitespace trimming, preserving case and leading zeroes. Numeric validation applies only to generated OMOP numeric IDs, not to source identifiers.

`src/second_scripts/person_id_map.py` validates the PERSON identity set and every existing event-table person link before changing any CSV. Duplicate PERSON source identities, missing identities and unmatched links stop processing. Unmatched event links are recorded in `combined_omop/identity-validation.json` using table names and one-based data-record ordinals, with no raw identifiers. The source CSVs remain unchanged on validation failure. This is a build blocker, not a policy to exclude those people or their records. Malformed PERSON identity/schema errors can stop earlier with an exception rather than that event-link report.

On success, the existing PERSON row-order numbering is retained and `person_id_mapping.csv` records the source-to-generated mapping. Row order can therefore still change numeric IDs between builds. Cross-release stable allocation remains a release-policy decision; this implementation does not claim to preserve published V1 IDs.

`src/second_scripts/transform_ids.py` accepts positive integer IDs and integral decimal representations such as `7.0`. It formats the existing `11` prefix plus seven-digit namespace exactly once. Missing, nonnumeric, fractional, negative, zero and oversized values raise instead of becoming plausible IDs or losing digits. All available tables are checked before writing transformed outputs. No rows are dropped and clinical fields are preserved by CSV reading/writing.

The historical composite visit-token formatting remains separate and unvalidated pending the visit design decision. This repair does not establish valid VISIT_OCCURRENCE links. It also does not repair date, concept, cohort or observation-period semantics. A passing synthetic identity test is not a validated OMOP dataset.

Run focused tests with `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python3 -m unittest discover -s tests -p 'test_identity.py'`. The tests cover opaque source identities, numeric representations, rejection cases, orphan failure without CSV mutation, intended source linkage, retained multiplicity, and row-order perturbation under the existing allocation rule.
