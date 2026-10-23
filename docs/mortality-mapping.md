# Mortality cause lookup

The mortality script reads clinical `source_tables/mortality.csv` for participant, date and source cause evidence. It reads `source_tables/usagi/mortality-cause-mapping.csv` for cause targets. The older participant-level `Mortality OMOP Mapping.csv` is no longer read or used as fallback.

Required lookup columns: diedcaus_key, icd10cm_key, cause_concept_id, mapping_status. Labels and decision notes may accompany them. Normalize cause by lowercase/trim/whitespace collapse and ICD by uppercase/removing whitespace. Do not discard punctuation or equate unknown with blank. Keys must be unique and normalized; status must be RETAINED or APPROVED; target must be a nonnegative integer. Unmatched pairs, duplicate keys and unresolved rows fail the run with audit evidence.

Death dates come exclusively from clinical `mortality.csv` dieddt plus the established conversion formula. No participant-specific date overrides are read. The four historical differences in the former mapping file have no documented correction rationale and are not carried forward.

Missing/nonintegral dates, dates before screening, unrepresentable dates and raw deaths preceding collected ALSFRS/vital-sign assessments are held in logs/mortality-reconciliation.json. No 1900 death date is invented. Check those holds before release. Same-person records with equal normalized cause/date produce one death and retain all source rows in the audit; conflicting repeats fail. Both assessment tables are required. The established identity stage continues to handle the previously approved missing-demographics exception.

The synthetic anchor remains 2016-01-01; the clinical dictionary describes death offsets relative to screening. The inherited death_type_concept_id 32851 and cause_source_value construction remain unchanged. Their provenance adequacy and CDM field length remain separate release limitations. This change does not certify the dataset for release.

Temporal screening compares collected assessments to the raw death date. Contradictory records are held with evidence rather than repaired by guessing another date.
