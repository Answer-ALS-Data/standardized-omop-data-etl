# Answer ALS Standardized OMOP Dataset

Release Version 1.0

# Summary

The Answer ALS Standardized OMOP Dataset provides a harmonized representation of the clinical data collected from more than one thousand participants in the [Answer ALS study](https://www.nature.com/articles/s41593-021-01006-0). The program generated clinical surveys, medical histories, ALSFRS R scores, environmental and family history questionnaires, and self reported genetic information. 

This release maps the clinical data to the OMOP Common Data Model Version 5.4 to support interoperability with other ALS datasets and to enable use of OHDSI tools for cohort creation and analysis. The ETL process followed Answer ALS mapping specifications and OHDSI guidance, with all mappings reviewed by data scientists and clinicians. ALS specific elements not yet present in OMOP vocabulary were implemented as temporary custom concepts, and detailed provenance information is retained through OMOP source value fields.

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

The source Answer ALS clinical dataset includes a set of structured study forms and logs that capture demographics, disease assessments, medical history, genetic survey responses, laboratory values, and vital signs. These source tables form the basis for the OMOP mappings in release version 1.0.

## Tables used in Version 1.0

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

* Medical\_History: Prior diagnoses and procedures reported by year.  
* ANSWER\_ALS\_Medications\_Log: Medications and supplements with dose, route, and frequency.

### Laboratory Measurements and Vital Signs

* ALS\_Gene\_Mutations: Self reported results for known ALS associated gene mutations.  
* Auxiliary\_Chemistry\_Labs: Supplemental lab values including uric acid, creatinine, phosphorus, CK, and related chemistries.  
* Vital\_Signs: Height, weight, temperature, blood pressure, heart rate, and BMI.

### Mortality

* Mortality: Date and cause of death for survival analyses.

# OMOP Conversion Overview

## Mapping Specifications

The [AALS OMOP Mapping Specifications (MVP)](https://docs.google.com/spreadsheets/d/16pI6KIVo-zGIo8Sdoqygbev7C_600JDpmEdok1BRBnY/edit?gid=1327264875#gid=1327264875) workbook was used to plan the mappings for all Version 1.0 tables. Each sheet in the workbook corresponds to an OMOP target table and lists the associated source tables, variable level mappings, target concept IDs, and any special considerations. These specifications guided the development of the ETL scripts hosted in the [Answer ALS standardized OMOP GitHub repository](https://github.com/Answer-ALS-Data/standardized-omop-data-etl).

## Vocabulary and Custom Concepts

Standard OMOP vocabularies were used whenever possible. Several ALS-specific elements—primarily diagnostic questions, clinical/EMG indicators from AALSDXFX, site-of-onset and bulbar dysfunction fields from AALSHXFX, and symptom-onset concepts for ALS and non-ALS motor neuron diseases—were not available in OMOP. Many are represented in NINDS CDEs and were submitted for future OMOP inclusion. Until official concepts are released, these items are captured using temporary custom concepts in the included concepts table to ensure faithful representation of the source data and compatibility with OMOP structure.

## Time and Date Handling

Relative day values in the source data were converted to calendar dates using an index date of 01 Jan 2016\. Source entries that provided only a year were mapped to 01 Jan of that year. Missing dates were assigned 01 Jan 1900\. Observation periods were defined using each participant’s first and last available record.

## Mapping Review and Tools

All mappings were created by data scientists and reviewed by a clinician. Larger lists of source terms were mapped using the OHDSI tool Usagi, with additional clinician review to confirm equivalence and resolve ambiguous terms.

## Source Value Formatting

OMOP fields ending in “\_source\_value” preserve the original content and context of the source variables. The formatting pattern is:

table1+var1 (var\_interpretation1): value1 (val\_interpretation1) | table2+var2 (var\_interpretation2): value2 (val\_interpretation2)

This provides a clear record of where each element originated.

## Missing Values Interpretation

In alignment with OMOP conventions, blank fields in the source data were not mapped. Explicit negative values such as “No” or “Negative” were mapped when provided. Empty fields were treated as missing information rather than an indication of absence.

# OMOP Table Summary

The table below summarizes which source tables contributed to each OMOP table in Version 1.0, along with the number of records and mapped concept identifiers.

| OMOP Table | Source Tables Pulled From | Unique Person IDs | Record Count | Unique Concept IDs |
| :---- | :---- | :---- | :---- | :---- |
| person | Demographics; subjects | 1040 | 1040 | N/A |
| death | Mortality | 400 | 400 | 17 |
| condition\_occurrence | AALSHXFX; Medical\_History; NEUROLOG | 1006 | 4797 | 884 |
| observation | AALSDXFX; AALSHXFX; ALSFRS\_R; Environmental\_Questionnaire; Family\_History\_Log | 1039 | 76590 | 61 |
| measurement | ALS\_Gene\_Mutations; Auxiliary\_Chemistry\_Labs; Vital\_Signs | 1016 | 21083 | 27 |
| drug\_exposure | ANSWER\_ALS\_Medications\_Log; Medical\_History | 908 | 7797 | 675 |
| procedure\_occurrence | Medical\_History | 327 | 647 | 244 |

# OMOP Table Level Notes

The following sections summarize how each source table contributed to the corresponding OMOP tables in Version 1.0. Notes highlight key mapping decisions, date handling and custom concepts.

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

* Date of death and cause of death were mapped.  
* When both general and specific causes were present, the more specific cause was used.  
* Conditions related to death were mapped using Usagi with clinician review.

## condition\_occurrence

### Source tables

AALSHXFX, Medical\_History, NEUROLOG

### Key points

* Medical History provided only the year. Dates were mapped using 01 Jan of that year, or 01 Jan 1900 if missing.  
* NEUROLOG terms were mapped using Usagi with clinician review. Free text “other” entries were preserved only in source fields.  
* ALS onset and Non-ALS MND onset were mapped using symptom onset dates from AALSHXFX.  
* Custom concepts were created for ALS onset and Non-ALS MND onset, due to missing vocabulary coverage.

## observation

### Source tables

AALSDXFX, AALSHXFX, ALSFRS\_R, Environmental\_Questionnaire, Family\_History\_Log

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
* Measurement technique was recorded when corresponding OMOP concepts existed.

## drug\_exposure

### Source tables

ANSWER\_ALS\_Medications\_Log, Medical\_History

### Key points

* Drug ingredients were mapped using Usagi with clinician review. Mapping equivalence is noted in source values.  
* Dose information was not mapped due to inconsistent formatting and frequent missing entries.  
* Missing or unrecognized medication values were mapped to No Matching Concept.  
* “Other” medication entries were not mapped in Version 1.0.

## procedure\_occurrence

### Source tables:

Medical\_History

### Key points

* Procedures reported in Medical History were mapped using Usagi with clinician review.  
* Dates were handled using the same year based rules as condition\_occurrence.  
* Free text “other” items were preserved only in the source fields.

# Unmapped Source Tables

The following Answer ALS source tables were not included in the Version 1.0 OMOP release. These tables were excluded due to limited vocabulary coverage, insufficient structure for standardized mapping, or because they are planned for inclusion in a future release.

## Strength and Functional Measurements

* Grip\_Strength\_Testing: Handgrip force measurements.  
* Hand\_Held\_Dynamometry: Quantitative muscle strength measurements.  
* Vital\_Capacity: Slow and forced vital capacity respiratory assessments.  
* Reflexes: Deep tendon and pathologic reflex evaluations.  
* Ashworth\_Spasticity\_Scale: Measures muscle tone and spasticity.

## Procedures, Devices, and Assisted Ventilation

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

The Version 1.0 release represents a minimum-viable OMOP conversion and includes several limitations that users should consider:

## Date Completeness

Many source forms did not contain full dates. Although standardized date rules were applied (see Data Transformation and Date Handling), temporal precision may be reduced for year-only or missing dates.

## Self-Reported Genetic Information

ALS gene mutation information is self-reported rather than derived from validated genomic testing. Only structured positive or negative responses were mapped.

## Unmapped Free Text

Free text entries under “Other” fields across several forms could not be reliably normalized and were therefore not mapped. These entries are retained only in \_source\_value fields.

## Medication Details

Dose, frequency, and route information were not mapped due to inconsistent formatting and frequent missingness. Only ingredient-level drug exposure is included in Version 1.0.

## Relationship Vocabulary Gaps

OMOP lacks concepts for some family relationships (e.g., half-siblings). These were mapped to the closest available standard terms; specific distinctions are preserved in source values but not standardized.

## Partial Table Coverage

Several structured forms were excluded from Version 1.0 because of limited vocabulary support or incomplete harmonization rules. These will be revisited for Version 2.0.

# Version Notes

## Version 1.0

Version 1.0 is the initial release of the Answer ALS OMOP dataset. It includes the essential clinical variables that could be consistently mapped to OMOP using established specifications and available vocabulary support. The goal of this release is to provide a reliable foundation for analysis and integration with other ALS datasets that use OMOP.

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