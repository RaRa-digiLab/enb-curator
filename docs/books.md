# Books
## Estonian National Bibliography - Curated Edition

General desc. here

RaRa Digilab 2024 etc

---
### id

*str: Unique identifier of the record*

**MARC source**: 001

---
### date_entered

*datetime: Date of the original creation of the record*

**MARC source**: 008

Date when the MARC record was first created. Impossible and missing dates are removed.

---
### isbn

*str: International Standard Book Number identifier*

**MARC source**: 020

Python library isbnlib is used to discard invalid ISBN numbers (i.e. cataloguing errors), ~0.3% of the data.

---
### creator

*str: Personal name used as a main entry in a bibliographic record, usually author, compiler, etc.*

**MARC source**: 100 (\$a, \$d, \$e)

The field is preprocessed during conversion from MARC21XML to tabular data (see `MARCrecordParser.handle_person_subfields()` in `.src/python/convert.py`).

Subfields a, d, e of the MARC field 100 are standardized as `Bornhöhe, Eduard (1862-1923) [autor]`.

---
### contributor

*str: Various other people marked on the work with a secondary role: translators, illustrators, editors etc. etc.*

**MARC source**: 700 (\$i, \$a, \$d, \$e, \$t)

The field is preprocessed during conversion from MARC21XML to tabular data (see `MARCrecordParser.handle_person_subfields()` in `.src/python/convert.py`).

Subfields a, d, e of the MARC field 100 are standardized as `Bornhöhe, Eduard (1862-1923) [autor]`.

Subfields i, t or both can be used to indicate the precise relationship of the person to the record (e.g. `Sisaldab: Kunnas, Leo (1967-): "Kustumatu valguse maailm"`)

---
### publisher

*str: Name of the publisher, distributor, etc.*

**MARC source**: 260\$b, 264\$b

In 2022, the cataloguing practices at the NLE changed and information previously noted on the MARC field 260 began to be noted on 264. As a result, the publisher field is concatenated from the respective subfields of these MARC fields.

---
### title

*str: Title of the work*

**MARC source**: 245\$a

---
### title_remainder

*str: Subtitle of the work or continuation of the title field*

**MARC source**: 245\$n

---
### title_part_nr

*str: Number of part/section of the work*

**MARC source**: 245\$n

---
### title_part_nr_cleaned

*str: Harmonized version of the title part number*

**MARC source**: 245\$n

Using regex, title part are converted to Arabic numerals with the name of the unit in square brackets (e. g. `Teine osa` -> `2 [osa]`) for easier filtering. The regular expression accounts for Roman and Arabic numerals, as well as ordinal numbers up to 10 in Estonian, English, Russian, German and French. The regular expression does not cover entries in other languages or those that do not contain numeration (`Lõpuosa`, `A`, `B`, etc.).

---
### title_varform

*str: Alternative/parallel title*

**MARC source**: 246\$a

This field can contain a parallel title marked on the publication, a commonly referred part of a longer title, or titles in the old Estonian orthography (generally pre-20th century), the field can contain the title in modern orthography. Titles in the original language are removed from 246$a and added to the title_original column.

---
### title_original

*str: Title in the original language for translated works*

**MARC source**: 246\$a, 240\$a, 130\$a

Original titles were marked together with everything else in 246\$a before 2022, after which 240\$a and 130\$a are used. This columns draws from all three MARC fields.

---
### title_freeform

*str: Freeform title that may also contain information about awards, provenance, etc.*

**MARC source**: 740\$a

---
### publication_date_control

*str: Publication date as marked in the control field of the record*

**MARC source**: 008

During cataloguing, unknown decimals are replaced with `u` (e. g. `191u` for a book published sometime in the 1910s).

---
### publication_date

*str: Publication date as marked in the relevant data field of the record*

**MARC source**: 260\$c, 264\$c

Compared to publication_date_control, this field is more closer to what is actually marked on the publication and can therefore contain some additional information than just the date (e. g. `c2006`, `tsens. 1890`). During cataloguing, unknown decimals are replaced with `-?` (e. g. `191-?` for a book published sometime in the 1910s). In some cases, a single publication can have several dates (e. g. `2023; ©2023`). If the date is not marked on the work and the relevant information comes from another source, the entry is wrapped in square brackets (`[1915]`).

In 2022, the cataloguing practices at the NLE changed and information previously noted on the MARC field 260 began to be noted on 264. As a result, the publication_date field is concatenated from the respective subfields of these MARC fields.

---
### publication_date_cleaned

*int: Cleaned version of publication_date*

**MARC source**: 260\$c, 264\$c

Publication date in integer form, best suited for analysis and filtering. A regular expression is used to capture the relevant year from publication_date. Uncertain dates (e. g. `191u`, `191-?`) are replaced with missing values.

---
### publication_decade

*int: Decade of publication for quicker filtering*

**MARC source**: 260\$c, 264\$c

The decade of the year marked in the publication_date field (e. g. `1960` for the years 1960-1969). Uncertain dates that are left empty in the publication_date_cleaned field still have their decade marked (e. g. `1910` for `191-?`). In cases where only the century in known (`16--?`), the publication_decade field is left empty.

---
### publication_place_control

*str: Publication place as marked in the control field of the record*

**MARC source**: 008

The country where or within the modern borders of which\* the work is published. Countries are marked with two or three letter codes which can be found here: [https://www.loc.gov/marc/countries/countries_name.html](https://www.loc.gov/marc/countries/countries_name.html)

\* MARC became the international standard in the 1970s and therefore bears some signs of this period. For example, Estonia and the Estonian S.S.R. have separate codes (`er` and `err`, respectively).

---
### publication_place

*str: Publication place as marked in the relevant data field of the record*

**MARC source**: 260\$a, 264\$a

The place of publication of the work, normally the city or other populated place. If additional details not marked on the publication are required for clarity, square brackets are used (`Laitse [Harjumaa]`, `Leeningrad [!]`, etc.).

In 2022, the cataloguing practices at the NLE changed and information previously noted on the MARC field 260 began to be noted on 264. As a result, the publication_place field is concatenated from the respective subfields of these MARC fields.

---
### added_intermediate_political_jurisdiction

Kas jätta?

---
### added_city

Kas jätta?

---
### place_of_manufacture

*str: Place of printing/manufacturing of the work*

**MARC source**: 260\$e

---
### manufacturer

*str: Name of the manufacturer*

**MARC source**: 260\$f

---
### corporate_unit

*str: Organization related to or responsible for publishing of the work*

**MARC source**: 710\$a, 710\$b

The corporate_unit field is combined from two MARC subfields that contain the corporate unit and subordinate unit, respectively (see `MARCrecordParser.handle_corporate_subfields()` in `.src/python/convert.py`). The latter are enclosed in square brackets after the name of the main organization (e. g. `Eesti Põllumajanduse Akadeemia [Maaparanduse kateeder]`).

---
### series_statement

*str: Title of the series to which the work belongs*

**MARC source**: 490\$a

---
### edition_statement

*str: Information relating to the edition of a work*

**MARC source**: 250\$a

---
### edition_n

*str: Harmonized version of the edition statement*

**MARC source**: 250\$a

Using regex, edition numbers are converted to Arabic numerals with the name of the unit in square brackets (`2. täiend. tr` -> `2 [tr]`, `2. Aufl` -> `2 [Aufl]`, etc.) for easier filtering. The regular expression accounts for Arabic numerals, as well as ordinal numbers up to 10 in Estonian, English, Russian and German. In the case of a repeated edition where the edition number is not explicitly mentioned, a `+` symbol is used (e. g. `Täiend. ja parand. tr` -> `+ [tr]`).

---
### original_distribution_year

*int: Year of publication of the first edition of the work*

**MARC source**: 534\$c

A regular expression is used to extract the year of the original edition from the original version note field (e. g. `Tartu : Eesti Kirjanduse Selts, 1933` -> `1933`). See also fields original_distribution_place and original_distribution_publisher.

---
### original_distribution_place

*str: Place of publication of the first edition of the work*

**MARC source**: 534\$c

A regular expression is used to extract the place of the original edition from the original version note field (e. g. `Tartu : Eesti Kirjanduse Selts, 1933` -> `Tartu`). See also fields original_distribution_year and original_distribution_publisher.

---
### original_distribution_publisher

*str: Publisher of the first edition of the work*

**MARC source**: 534\$c

A regular expression is used to extract the place of the original edition from the original version note field (e. g. `Tartu : Eesti Kirjanduse Selts, 1933` -> `Eesti Kirjanduse Selts`). See also fields original_distribution_year and original_distribution_place.

---
### language

*str: Main language of the work as marked in the control field of the record*

**MARC source**: 008

Languages are marked with three letter codes from the MARC code list(mostly overlaps with the ISO standard): [https://www.loc.gov/marc/languages/language_name.html](https://www.loc.gov/marc/languages/language_name.html)

---
### language_additional

*str: Language code of the work as marked in the relevant data field of the record*

**MARC source**: 041\$a

The language_additional field is used if the work contains more than one language (parallel text, summaries etc.). In the case of translated works, in which the field contains the target language (see language_original for the source language). Detailed comments about the entries can be found in the language_note field.

Languages are marked with three letter codes from the MARC code list (mostly overlaps with the ISO standard): [https://www.loc.gov/marc/languages/language_name.html](https://www.loc.gov/marc/languages/language_name.html)

---
### language_original

*str: Original language of the work*

**MARC soruce**: 041\$h

In the case of translated works, the source language is marked on the language _original field (see language_additional for the target language).

Languages are marked with three letter codes from the MARC code list(mostly overlaps with the ISO standard): [https://www.loc.gov/marc/languages/language_name.html](https://www.loc.gov/marc/languages/language_name.html)

---
### language_note

*str: Comments about the use of the language and language_additional fields*

**MARC source**: 546\$a

Textual information on the language(s) of the work.

---
### is_fiction

*bool: Whether the work is considered fiction*

**MARC source**: 008

Generic identification of whether or not the item is a work of fiction. Only applies to books.

---
### is_posthumous

*bool: Whether the work is published posthumously*

**MARC source**: 100\$d, 260\$c, 264\$c

A comparison between the death dates in `creator` and `publication_dates_cleaned`. Please note that this takes into account only the main creator of the record - coauthors who may be sometimes found under `contributor` with the role `[autor]` are excluded from the calculation (c.f. `check_if_posthumous` in `clean.py` for more details). Where `is_posthumous` cannot be meaningfully calculated, an empty value is returned.
---
### udc

*str: Universal Decimal Classification number of the work*

**MARC source**: 080\$a

The Universal Decimal Classification (UDC) is a bibliographic and library classification system that organizes all fields of knowledge into a hierarchical structure of numerical codes. It is widely used in libraries and information services to facilitate the retrieval and management of information.

---
### topic_keyword

*str: Keywords about the topic, theme or contents of the work and their thesaurus links*

**MARC source**: 650\$a, 650\$0

The topic_keyword field is combined from two MARC subfields that contain the keyword and its thesaurus link, respectively (see `MARCrecordParser.handle_keyword_subfields()` in `.src/python/convert.py`). Most of the keywords come from the [Estonian Subject Thesaurus](https://ems.elnet.ee/index.php) (EMS), a universal controlled vocabulary for indexing and searching various library material.

EMS identifers are added in square brackets after the relevant keyword (`õigusajalugu [EMS010783]`, `loodusvarad [EMS004541]` etc.). The keywords can be accessed as linked data in the EMS by prepending `https://ems.elnet.ee/id/` to the identifier (e. g. `õigusajalugu [EMS010783]` -> `https://ems.elnet.ee/id/EMS010783`).

---
### genre_keyword

*str: Keywords about the genre of the work and their thesaurus links*

**MARC source**: 655\$a, 655\$0

The genre_keyword field is combined from two MARC subfields that contain the keyword and its thesaurus link, respectively (see `MARCrecordParser.handle_keyword_subfields()` in `.src/python/convert.py`). Most of the keywords come from the [Estonian Subject Thesaurus](https://ems.elnet.ee/index.php) (EMS), a universal controlled vocabulary for indexing and searching various library material.

EMS identifers are added in square brackets after the relevant keyword (`mälestused [EMS009112]`, `muinasjutud [EMS009202]` etc.). The keywords can be accessed as linked data in the EMS by prepending `https://ems.elnet.ee/id/` to the identifier (e. g. `mälestused [EMS009112]` -> `https://ems.elnet.ee/id/EMS009112`).

---
### geographic_keyword

*str: Keywords about geographic locations that are the subject of the work and their thesaurus links*

**MARC source**: 651\$a, 651\$0

The geographic_keyword field is combined from two MARC subfields that contain the keyword and its thesaurus link, respectively (see `MARCrecordParser.handle_keyword_subfields()` in `.src/python/convert.py`). Most of the keywords come from the [Estonian Subject Thesaurus](https://ems.elnet.ee/index.php) (EMS), a universal controlled vocabulary for indexing and searching various library material.

EMS identifers are added in square brackets after the relevant keyword (`Eesti (riik) [EMS131705]`, `Põlvamaa [EMS131732]` etc.). The keywords can be accessed as linked data in the EMS by prepending `https://ems.elnet.ee/id/` to the identifier (e. g. `Eesti (riik) [EMS131705]` -> `https://ems.elnet.ee/id/EMS131705`).

---
### chronological_keyword

*str: Keywords about time periods that are the subject of the work*

**MARC source**: 648\$a

Chronological keywords mark the relevant century, decade, year or other period that the work is about (`20. sajandi 2. pool`, `1980-ndad`, `1989`, etc.).

---
### corporate_keyword

*str: Organizations that are the subject of the work or related to the work but not directly responsible for its publication*

**MARC source**: 610\$a

---
### person_keyword

*str: Persons that are the subject of the work or related to the work but not directly responsible for its publication*

**MARC source**: 600\$a, 600\$d, 600\$e, 600\$t

The person_keyword field is preprocessed during conversion from MARC21XML to tabular data (see `MARCrecordParser.handle_person_subfields()` in `.src/python/convert.py`).

Subfields a, d, e of the MARC field 600 are standardized as `Petersen, Wilhelm (1854-1933) [omanik]`. In the case of books, "owner" is the only role (650\$e); it is mostyl used for older/rare books.

The subfield t can be used to mark another work, authored by the person marked in the person_keyword field (e.g. `Vilde, Eduard (1865-1933): "Mahtra sõda"` for a work that mentions or treats "Mahtra sõda" by E. Vilde).

---
### page_count

*int: Number of pages*

**MARC source**: 300\$a

Page counts are extracted from the physical description field using a regular expression.

---
### illustrated

*bool: Whether the work contains illustrations*

**MARC source**: 300\$b

---
### physical_size

*int: Height of the work in cm (rounded up)*

**MARC source**: 300\$c

Physical size is measured from the vertical dimension of the publication and always rounded up (`20,1 cm` -> `21 cm`). The listed size always refers to the height, even if the book's width is greater than its height.

---
### print_run

*int: Total number of copies of the work or its given edition that were produced*

**MARC source**: 500\$a

---
### price

*str: Price marked on publications with a fixed price*

**MARC source**: 500\$a

---
### typeface

*str: Whether the work uses Roman type (a) or Fraktur (f)*

**MARC source**: 500\$a

---
### bibliography_register

*str: Whether the work contains a bibliography (b), a register (r) or both (br)*

**MARC source**: 504\$a

---
### copyright_status

*str: Known copyright status of the work*

**MARC source**: 542\$l

Most works do not have a marked copyright status because the field is still being updated in the ENB.

---
### digitized

*bool: Whether the work has been digitized*

**MARC source**: 533\$a

---
### digitized_year

*str: When the work has been digitized*

**MARC source**: 533\$d

Note: the common entry `2019-2021` corresponds to the mass digitization project that took place during these years.

---
### access_uri

**MARC source**: 856\$u

Access to the digitized version of the work.