# Persons
## Estonian National Bibliography - Curated Edition

RaRa Digilab 2024 etc

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

**MARC source**: 375\$a

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