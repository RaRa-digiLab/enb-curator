# Persons
## Estonian National Bibliography - Curated Edition

> **Authors**: Krister Kruusmaa, Peeter Tinits, Laura Nemvalts  
> **Institution**: National Library of Estonia  
> **License**: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

This curated dataset is derived from the persons authority file of the Estonian National Bibliography (ENB), a comprehensive catalog of publications written in Estonian, published in Estonia, or focusing on Estonian culture and people. Designed for computational analysis, this dataset adapts the original authority file for research and cultural exploration. Through a systematic process of filtering, cleaning, and harmonizing, the ENB dataset is presented in a streamlined tabular format that retains rich metadata while improving accessibility. Fields selected for inclusion are harmonized and, where possible, linked to external sources, offering an optimized and reproducible resource for historical, cultural, and bibliographic research.

---

## Columns

Below is a description of each column in the dataset.

---
### id

*str: Unique identifier of the record*

**MARC source**: 001

---
### creator

*str: Personal name in the common form used across the Curated ENB datasets*

**MARC source**: 100 (\$a, \$d)

Use this field to link to personal names appearing in the `creator` and `contributor` fields of records.

The field is preprocessed during conversion from MARC21XML to tabular data (see `MARCrecordParser.handle_person_subfields()` in `.src/python/convert.py`).

Subfields a and d of the MARC field 100 are standardized as `Bornhöhe, Eduard (1862-1923)`.

---
### name

*str: Only the surname and given name(s) of the person*

**MARC source**: 100 (\$a, \$d)

---
### birth_year

*str: Birth year of the person*

**MARC source**: 100 (\$d)

---
### death_year

*str: Death year of the person*

**MARC source**: 100 (\$d)

---
### profession

*str: Main profession of the person*

**MARC source**: 374\$a

---
### gender

*str: Gender of the person*

**MARC source**: 375\$a (Additionally, the data is enriched via VIAF, linked from `001`)

Enrichment from VIAF is retrieved from the VIAF Authority Cluster endpoint.

---
### name_varform

*str: Alternative forms of the person's name as they may appear on publications*

**MARC source**: 400\$a

Other Curated ENB datasets always use the standardized nameform (`creator` in this table) in the `creator` and `contributor` fields.

---
### geographic_iso

*str: ISO code of the person's main area of activity*

**MARC source**: 043\$c

---
### biographical_info

*str: Short freeform biography of the person*

**MARC source**: 680\$i

---
### viaf_id

*str: VIAF identifier of the record*

**MARC source**: None (linked from `001`)

Retrieved from the VIAF Authority Cluster endpoint.

---
### wkp_id

*str: Wikidata identifier of the record*

**MARC source**: None (linked from `001`)

Retrieved from the VIAF Authority Cluster endpoint.
