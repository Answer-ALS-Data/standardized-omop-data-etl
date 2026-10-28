# Medical-history dates

V1.1 interprets `medhxyr` as days since intake by default. No date-profile file is required in the release source tables.

For historical regression only, an optional `source_tables/medical_history_date_profile.json` can explicitly select `years` or `relative-days`. The isolated runner's `--medical-history-profile` option supplies this override (an input-preparation `preparation.json` is accepted). Invalid profiles fail rather than silently falling back. Never select units from filenames or numeric magnitude.

For the confirmed relative-day profile, `medhxyr` means days since intake. Valid integer offsets are added to the synthetic 2016-01-01 intake anchor, including zero and negative offsets. Missing values and invalid/unrepresentable offsets use the established 1900-01-01 sentinel and retain mapped records. The sentinel is a placeholder, not a diagnosis date. There is no arbitrary negative-offset cutoff. The old `years` profile preserves the previous `year_to_date` behavior for regression and historical use.

Condition, Drug, Procedure and Observation use the same conversion. The retained Device script also uses it, but Device output is deferred to V2. Source provenance includes the original CSV date value, its units, and valid/missing/invalid status; Device now also includes date provenance. Private `logs/medical-history-dates-DOMAIN.json` files pin input hashes (and profile hashes when an override is supplied) and record original data-row ordinals, raw values, derived dates and emitted mapping counts. Counts repeat source rows across the four domain audits: sum emitted contributions when appropriate, not source-row counts. Keep these audits with controlled run artifacts, outside public Git.

The Drug mapping continues copying its start date into end and verbatim-end dates. This preserves the existing convention; it does not establish an actual treatment duration. The existing observation-period filter excludes 1900-01-01, covered by a focused regression. Broader observation-period meaning remains separate integration work.

Only explicit profile selection changes the interpretation. Upstream year-only imputation must not be repeated after receiving relative-day values. The implementation does not resolve unmapped terms, duplicate-event policy, vocabulary validity or other existing quality issues.
