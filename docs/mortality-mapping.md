# Mortality cause lookup

Clinical `source_tables/mortality.csv` supplies participant, date and cause evidence. `source_tables/usagi/mortality-cause-mapping.csv` supplies only cause concepts. The historical participant-level Mortality OMOP Mapping.csv is inactive and is never a fallback. No date-override file is read.

Required lookup columns: diedcaus_key, icd10cm_key, cause_concept_id, mapping_status. Normalize cause by lowercase/trim/whitespace collapse and ICD by uppercase/removing whitespace. Preserve punctuation and blank versus explicit unknown. Duplicate/noncanonical lookup keys, unresolved statuses or unmatched source pairs fail explicitly rather than selecting an arbitrary concept.

## Legacy mortality behavior retained

Emit one record per clinical mortality source row, in source order. Do not deduplicate by participant or reject conflicting repeated source rows. Duplicate lookup keys remain an error: this is distinct from repeated clinical records.

Death dates use the original pandas missing-value recognition and the 2016-01-01 anchor plus the relative days. Missing dates emit 1900-01-01. Negative offsets and death dates before assessments are not excluded or replaced. Nonmissing values that cannot be converted still fail rather than being silently repaired. Cause source text retains the original date/cause/code construction; if date is absent its date component is omitted. Death type remains 32851.

logs/mortality-reconciliation.json retains each original row, chosen mapping, converted date and report-only warnings for missing dates, repeated participants and chronology contradictions. Optional ALSFRS/vital-sign inputs support those reports; they are not required for conversion. These findings do not create holds or alter the output.

The separately approved identity stage is unchanged: the previously reviewed missing-demographics exclusion applies only to its named participant. Other missing PERSON links still block remapping. Restoring sentinel behavior exposes another such September record; no new exclusion is authorized by this mapping change.

This scope does not establish a source deduplication policy or certify death provenance, field lengths or downstream observation-period semantics. The original behavior and source limitations are retained at the user's direction.
