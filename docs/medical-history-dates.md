# Medical-history dates

The four medical-history domain scripts require `source_tables/medical_history_date_profile.json` containing an explicit `medical_history_units` value: `years` or `relative-days`. The existing input-preparation `preparation.json` can be used as this profile. Pass it to the isolated runner with `--medical-history-profile /path/to/preparation.json`; the runner copies and hashes it. Never select date meaning from filenames or numeric magnitude.

For the confirmed relative-day profile, `medhxyr` means days since intake. Valid integer offsets are added to the synthetic 2016-01-01 intake anchor, including zero and negative offsets. Missing values and invalid/unrepresentable offsets use the established 1900-01-01 sentinel and retain mapped records. The sentinel is a placeholder, not a diagnosis date. There is no arbitrary negative-offset cutoff. The old `years` profile preserves the previous `year_to_date` behavior for regression and historical use.

Condition, Drug, Procedure and Device use the same conversion. Source provenance includes the original CSV date value, its units, and valid/missing/invalid status; Device now also includes date provenance. Private `logs/medical-history-dates-DOMAIN.json` files pin input/profile hashes and record original data-row ordinals, raw values, derived dates and emitted mapping counts. Counts repeat source rows across the four domain audits: sum emitted contributions when appropriate, not source-row counts. Keep these audits with controlled run artifacts, outside public Git.

The Drug mapping continues copying its start date into end and verbatim-end dates. This preserves the existing convention; it does not establish an actual treatment duration. The existing observation-period filter excludes 1900-01-01, covered by a focused regression. Broader observation-period meaning remains separate integration work.

Only explicit profile selection changes the interpretation. Upstream year-only imputation must not be repeated after receiving relative-day values. The implementation does not resolve unmapped terms, duplicate-event policy, vocabulary validity or other existing quality issues.
