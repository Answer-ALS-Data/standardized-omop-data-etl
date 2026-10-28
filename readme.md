# Answer ALS Standardized OMOP Dataset

Release Version 1.1

# Summary

The Answer ALS Standardized OMOP Dataset provides a harmonized representation of the clinical data collected from more than one thousand participants in the [Answer ALS study](https://www.nature.com/articles/s41593-021-01006-0). The program generated clinical surveys, medical histories, ALSFRS R scores, environmental and family history questionnaires, and self reported genetic information. 

This release maps the clinical data to the OMOP Common Data Model Version 5.4 to support interoperability with other ALS datasets and to enable use of OHDSI tools for cohort creation and analysis. The ETL process followed Answer ALS mapping specifications and OHDSI guidance, with the original mappings reviewed by data scientists and clinicians and subsequent V1.1 changes reviewed during release development. ALS specific elements not yet present in OMOP vocabulary were implemented as temporary custom concepts, and detailed provenance information is retained through OMOP source value fields.

*Note: In addition to the clinical dataset described here, Answer ALS provides extensive participant multiomic data and biosamples (e.g., iPSC lines, PBMCs, plasma, serum, and CSF), accessible via the [Neuromine portal](https://dataportal.answerals.org/).*

# Cohort Overview

The Answer ALS Standardized OMOP Dataset includes 1,040 participants with demographic and disease status distributions shown below.

|  Demographics and Disease Status | Count | Percent |
| :---- | :---- | :---- |
| Total | 1040 | 100.0% |
| Gender: Male | 617 | 59.3% |
| Gender: Female | 423 | 40.7% |
| Age: 18-29 | 17 | 1.6% |
| Age: 30-39 | 51 | 4.9% |
| Age: 40-49 | 106 | 10.2% |
| Age: 50-59 | 284 | 27.3% |
| Age: 60-69 | 368 | 35.4% |
| Age: 70-79 | 185 | 17.8% |
| Age: 80+ | 29 | 2.8% |
| Race: White | 955 | 91.8% |
| Race: Black/African American | 49 | 4.7% |
| Race: Asian | 24 | 2.3% |
| Race: Other/Unknown | 12 | 1.2% |
| Disease Status: ALS | 852 | 81.9% |
| Disease Status: Asymptomatic ALS Gene Carrier | 12 | 1.2% |
| Disease Status: Healthy Control | 107 | 10.3% |
| Disease Status: Non-ALS MND | 69 | 6.6% |

# Source Data Overview

The source Answer ALS clinical dataset includes a set of structured study forms and logs that capture demographics, disease assessments, medical history, genetic survey responses, laboratory values, and vital signs. These source tables form the basis for the OMOP mappings in release version 1.1.

## Tables used in Version 1.1

### Demographics and Subject Classification

* Demographics: Age, sex, race, and ethnicity.  
* subjects: Disease group assignments for ALS, Non-ALS Motor Neuron Disease, Asymptomatic Gene Carriers, and Controls.

### Diagnostic and Clinical Assessment Forms

* AALSDXFX: Diagnostic confirmation using clinical, EMG, and imaging criteria, including regional upper and lower motor neuron findings and Revised El Escorial classification.  
* AALSHXFX: Symptom onset date, diagnosis date, and site of onset.  
* ALSFRS\_R: Functional scores across 12 domains for tracking ALS progression.  
* NEUROLOG: Neurological diagnoses with dates and specified subtypes.  
* Environmental\_Questionnaire: Lifestyle, occupational exposures, and environmental factors.  
* Family\_History\_Log: Diseases reported among relatives and self reported genetic testing results.

### Medical History and Medications

* Medical\_History: Prior diagnoses, procedures and medications, with dates supplied as days relative to intake in the updated pull.
* ANSWER\_ALS\_Medications\_Log: Medications and supplements with dose, route, and frequency.

### Laboratory Measurements and Vital Signs

* ALS\_Gene\_Mutations: Self reported results for known ALS associated gene mutations.  
* Auxiliary\_Chemistry\_Labs: Supplemental lab values including uric acid, creatinine, phosphorus, CK, and related chemistries.  
* Vital\_Signs: Height, weight, temperature, blood pressure, heart rate, and BMI.

### Mortality

* Mortality: Date and cause of death for survival analyses.

# OMOP Conversion Overview

## Mapping Specifications

The [AALS OMOP Mapping Specifications (MVP)](https://docs.google.com/spreadsheets/d/16pI6KIVo-zGIo8Sdoqygbev7C_600JDpmEdok1BRBnY/edit?gid=1327264875#gid=1327264875) workbook was used to plan the mappings for all Version 1.0 tables. Each sheet in the workbook corresponds to an OMOP target table and lists the associated source tables, variable level mappings, target concept IDs, and any special considerations. Detailed variable mappings and rationale belong in this workbook. Reconciliation of the workbook with the V1.1 implementation is pending. These specifications guided the development of the ETL scripts hosted in the [Answer ALS standardized OMOP GitHub repository](https://github.com/Answer-ALS-Data/standardized-omop-data-etl).

## Vocabulary and Custom Concepts

V1.1 pins Athena vocabulary release **v5.0 29-AUG-26**. Standard OMOP vocabularies were used whenever possible. Several ALS-specific elements—primarily diagnostic questions, clinical/EMG indicators from AALSDXFX, site-of-onset and bulbar dysfunction fields from AALSHXFX, and symptom-onset concepts for ALS and non-ALS motor neuron diseases—were not available in OMOP. Many are represented in NINDS CDEs and were submitted for future OMOP inclusion. Until official concepts are released, these items are captured using temporary custom concepts in the included concepts table to ensure faithful representation of the source data and compatibility with OMOP structure.

The output `concept.csv` contains 34 local definitions, not the complete standard vocabulary. A database load also requires the pinned standard vocabulary tables and supporting metadata for `ALS_Custom`; these local concepts are not designated standard concepts.

## Time and Date Handling

Relative day values are converted using the synthetic intake anchor 01 Jan 2016. In the updated medical-history pull, `medhxyr` contains days since intake, including valid negative offsets; the ETL treats these values as relative days by default and retains an optional year-based override for historical inputs. Missing event dates use 01 Jan 1900 as an unknown-date placeholder; medical-history offsets that cannot be represented as dates use the same fallback. This rule does not supply a birth year: participants without a birth year and their linked records are excluded (one participant in this pull).

Observation periods retain the existing first/last available event-date calculation, excluding the 1900 sentinel. They may include historical events and should not be interpreted as continuous study follow-up or healthcare observation; existing fallback periods are synthetic.

## Mapping Review and Tools

The original mappings were created by data scientists and reviewed by a clinician. V1.1 retains those decisions where applicable and incorporates reviewed vocabulary replacements and corrections; mapping equivalence and retained approximations should be consulted alongside the mapped concepts. Larger lists of source terms were mapped using the OHDSI tool Usagi, with additional clinician review to confirm equivalence and resolve ambiguous terms.

## Source Value Formatting

OMOP fields ending in “\_source\_value” preserve the original content and context of the source variables. The formatting pattern is:

table1+var1 (var\_interpretation1): value1 (val\_interpretation1) | table2+var2 (var\_interpretation2): value2 (val\_interpretation2)

This provides a clear record of where each element originated.

## Missing Values Interpretation

Blank source responses are not interpreted as negative results. A missing qualifier does not necessarily discard an otherwise usable result; for example, numeric laboratory results with missing normality are retained with unknown normality. Explicit negative values such as “No” or “Negative” were mapped when provided. Empty fields were treated as missing information rather than an indication of absence.

# OMOP Table Summary

The table below summarizes which source tables contributed to each OMOP table in V1.1, along with the number of records and distinct primary concept identifiers. Concept counts include concept 0 where present; DEATH uses cause_concept_id. One retained repeated mortality record accounts for the difference between DEATH records and people.

| OMOP Table | Source Tables Pulled From | Unique Person IDs | Record Count | Unique Concept IDs |
| :---- | :---- | :---- | :---- | :---- |
| person | Demographics; subjects | 1040 | 1040 | N/A |
| death | Mortality | 552 | 553 | 17 |
| condition\_occurrence | AALSHXFX; Medical\_History; NEUROLOG | 1006 | 4806 | 867 |
| observation | AALSDXFX; AALSHXFX; subjects; ALSFRS\_R; Environmental\_Questionnaire; Family\_History\_Log; Medical\_History | 1039 | 78064 | 66 |
| measurement | ALS\_Gene\_Mutations; Auxiliary\_Chemistry\_Labs; Vital\_Signs | 1018 | 21129 | 34 |
| drug\_exposure | ANSWER\_ALS\_Medications\_Log; Medical\_History | 908 | 7795 | 666 |
| procedure\_occurrence | Medical\_History | 329 | 648 | 233 |
| observation\_period | Derived from available event dates | 1040 | 1040 | N/A |

# OMOP Table Level Notes

The following sections summarize how each source table contributed to the corresponding OMOP tables in V1.1. Notes highlight key mapping decisions, date handling and custom concepts.

## person

### Source tables:

Demographics, subjects

### Key points

* Race values were mapped when a single race was reported. Multiple or blank entries were mapped to No Matching Concept.  
* The gender\_source\_value includes omic inferred sex if it differed from the recorded sex from the survey.  
* Only demographic variables required by OMOP were included.

## death

### Source tables:

Mortality

### Key points

* Death dates use source relative-day values and the existing missing-date fallback; the cause mapping does not override dates.
* Causes use a participant-independent lookup of the source cause text and ICD-10-CM combination, preserving reviewed decisions and favoring the more immediate cause when available.
* Conditions related to death were mapped using Usagi with clinician review.

## condition\_occurrence

### Source tables

AALSHXFX, Medical\_History, NEUROLOG

### Key points

* Medical History dates use days relative to intake for V1.1 (see Time and Date Handling).
* NEUROLOG terms were mapped using Usagi with clinician review. Free text “other” entries were preserved only in source fields.  
* ALS onset and Non-ALS MND onset were mapped using symptom onset dates from AALSHXFX.  
* Custom concepts were created for ALS onset and Non-ALS MND onset, due to missing vocabulary coverage.

## observation

### Source tables

AALSDXFX, AALSHXFX, subjects, ALSFRS\_R, Environmental\_Questionnaire, Family\_History\_Log, Medical\_History

### Key points

* AALSDXFX diagnostic questions (alsdx1, alsdx2, alsdx3) are not currently in OMOP vocabulary. They were represented as custom concepts derived from NINDS CDEs.  
* The question associated with alsdx1 contains components of three El Escorial questions, so each instance was mapped to all three concepts.  
* Clinical and EMG indicators in AALSDXFX were also added as custom concepts.  
* Site of onset and bulbar dysfunction from AALSHXFX were mapped using custom concepts.  
* Family History mapped relationships to existing OMOP concepts. Half siblings were mapped as full siblings due to vocabulary limitations. Paternal and maternal modifiers were kept only for grandparents.  
* “Other” free text items were not mapped but were retained in source values.

## measurement

### Source tables

ALS\_Gene\_Mutations, Auxiliary\_Chemistry\_Labs, Vital\_Signs

### Key points

* Gene mutation results were self reported. Only structured positive or negative fields were mapped. Free text was retained only as a source value.  
* Chemistry lab units were converted to mg/dL when possible. Enzymatic “units per liter” were retained without conversion.  
* Vital signs were standardized:  
  * Temperatures converted to Celsius  
  * Lengths converted to centimeters  
  * Weight converted to kilograms  
* Measurement technique was recorded when corresponding OMOP concepts existed. Blood-pressure position now selects a sitting, standing or supine systolic/diastolic concept; the result remains numeric. Missing position retains a generic blood-pressure concept.
* Weight and height measurements without source units are omitted; units are not inferred from other visits.

## drug\_exposure

### Source tables

ANSWER\_ALS\_Medications\_Log, Medical\_History

### Key points

* Drug ingredients were mapped using Usagi with clinician review. Mapping equivalence is noted in source values.  
* Dose and frequency are retained in source values; recognized routes are mapped to route concepts.
* Missing or unrecognized medication values were mapped to No Matching Concept.  
* “Other” medication entries were not mapped in Version 1.0.

## procedure\_occurrence

### Source tables:

Medical\_History

### Key points

* Procedures reported in Medical History were mapped using Usagi with clinician review.  
* Dates use the same relative-day rules as condition\_occurrence.
* Free text “other” items were preserved only in the source fields.

# Unmapped Source Tables

The following Answer ALS source tables remain outside the V1.1 OMOP release. These tables were excluded due to limited vocabulary coverage, insufficient structure for standardized mapping, or because they are planned for inclusion in a future release.

## Strength and Functional Measurements

* Grip\_Strength\_Testing: Handgrip force measurements.  
* Hand\_Held\_Dynamometry: Quantitative muscle strength measurements.  
* Vital\_Capacity: Slow and forced vital capacity respiratory assessments.  
* Reflexes: Deep tendon and pathologic reflex evaluations.  
* Ashworth\_Spasticity\_Scale: Measures muscle tone and spasticity.

## Procedures, Devices, and Assisted Ventilation

* Device exposures: Deferred to Version 2.0.

* Feeding\_Tube\_Placement: Gastrostomy procedure dates and clinical rationale.  
* Tracheostomy: Procedure dates, hospitalization details, and indications.  
* NIV\_Log: Non-invasive ventilation use, including dates and duration.  
* Permanent\_Assisted\_Ventilation: Records initiation of permanent ventilation.  
* Diaphragm\_Pacing\_System\_Device: Recommendation and placement dates for pacing systems.

## Cognitive, Behavioral, and Neuropsychological Assessments

* ALS\_CBS: ALS Cognitive Behavioral Scale and caregiver questionnaire.  
* CNS\_Lability\_Scale: Assessment of emotional lability and pseudobulbar affect.

## Sample and Biospecimen Collection

* Cerebrospinal\_Fluid: CSF sample collection and processing details.  
* PBMC\_Sample\_Collection: PBMC processing and collection metadata.  
* Plasma\_Sample: Plasma sample collection and handling information.  
* Serum\_Sample: Serum sample collection and handling information.  
* DNA\_Sample\_Collection: DNA sample collection metadata.

## Administrative Forms

* ANSASFD: Study completion and disposition status.

# Known Limitations

The V1.1 release retains the original scope of the OMOP conversion and includes several limitations that users should consider:

## Date Completeness

Many source forms did not contain full dates. Although standardized date rules were applied (see Data Transformation and Date Handling), temporal precision may be reduced for year-only or missing dates.

## Self-Reported Genetic Information

ALS gene mutation information is self-reported rather than derived from validated genomic testing. Only structured positive or negative responses were mapped.

## Unmapped Free Text

Free text entries under “Other” fields across several forms could not be reliably normalized and were therefore not mapped. These entries are retained only in \_source\_value fields.

## Medication Details

Dose and frequency are retained in source values rather than standardized dosing fields. Recognized routes are mapped to route concepts, with the original route retained in source values. Medication mappings primarily represent ingredients; retained approximations and their equivalence are documented in source values and mapping reviews.

## Relationship Vocabulary Gaps

OMOP lacks concepts for some family relationships (e.g., half-siblings). These were mapped to the closest available standard terms; specific distinctions are preserved in source values but not standardized.

## Partial Table Coverage

Several structured forms remain excluded from V1.1 because of limited vocabulary support or incomplete harmonization rules. These will be revisited for Version 2.0.

## Source and Export Limitations

Source inconsistencies are generally preserved rather than repaired by the ETL, including reversed medication intervals and events after recorded death. These are reported in validation. Detailed provenance can exceed standard OMOP string lengths; integer fields may serialize with decimal notation, and DEATH includes an extra `death_id`. Export-format remediation is deferred to V2. The CSV outputs are not yet a validated strict-schema database load.

# Version Notes

## Version 1.0

Version 1.0 is the initial release of the Answer ALS OMOP dataset. It includes the essential clinical variables that could be consistently mapped to OMOP using established specifications and available vocabulary support. The goal of this release is to provide a reliable foundation for analysis and integration with other ALS datasets that use OMOP.

## Version 1.1

V1.1 processes the 17 September 2026 pull with the existing source-form coverage. It incorporates corrected medical-history relative dates, stable person-ID handling and reviewed exclusions, participant-independent mortality mapping, vocabulary updates, and targeted response, unit and measurement corrections.

## Version 2.0 (Planned)

Version 2.0 is planned as an expansion of this initial release. Additional tables and variables that were not mapped in Version 1.0 will be evaluated for inclusion, along with any vocabulary or mapping updates needed to support them. The scope will be determined based on feasibility, data completeness, and alignment with OMOP conventions.

# Appendix: OMOP Resource Guide

This section provides a brief set of key resources for readers who want to explore the OMOP ecosystem further.

[OMOP CDM Overview](https://ohdsi.github.io/CommonDataModel/background.html): High-level background on the purpose and design of the Common Data Model.

[CDM Table Diagram](https://ohdsi.github.io/CommonDataModel/index.html): Visual map of the core OMOP tables and their relationships.

[CDMv5.4 Specifications](https://ohdsi.github.io/CommonDataModel/cdm54.html): Detailed definitions of all OMOP tables, fields, and conventions.

[OHDSI Website](https://ohdsi.org/): Central hub for the community developing OMOP and its open-source tools.

[OHDSI Forums](https://forums.ohdsi.org/): Public discussions on implementation, research, and troubleshooting.

[Book of OHDSI](https://ohdsi.github.io/TheBookOfOhdsi/): Comprehensive guide to standardized health data and analytics workflows.

[OHDSI GitHub](https://github.com/OHDSI): Code repositories for tools, vocabularies, and reference ETL resources.

[OHDSI YouTube](https://www.youtube.com/@OHDSI): Educational talks, tutorials, and conference sessions on OMOP and OHDSI.

[OHDSI Wiki](https://www.ohdsi.org/web/wiki/doku.php): Documentation of methods, vocabularies, and collaborative community projects.
