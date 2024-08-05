## id

*Unique identifier of the record*

**MARC source**: 001

## isbn

*International Standard Book Number identifier*

**MARC source**: 020

Python library isbnlib is used to discard invalid ISBN numbers (i.e. cataloguing errors), ~0.3% of the data.

## creator

*Personal name used as a main entry in a bibliographic record, usually author, compiler, etc.*

**MARC source**: 100 (\$a, \$d, \$e)

The field is preprocessed during conversion from MARC21XML to tabular data (see `MARCrecordParser.handle_person_subfields()` in `.src/python/convert.py`).

Subfields a, d, e of the MARC field 100 are standardized as `Bornhöhe, Eduard (1862-1923) [autor]`.

## contributor

*Various other people marked on the publication with a secondary role: translators, illustrators, editors etc. etc.*

**MARC source**: 700 (\$i, \$a, \$d, \$e, \$t)

The field is preprocessed during conversion from MARC21XML to tabular data (see `MARCrecordParser.handle_person_subfields()` in `.src/python/convert.py`).

Subfields a, d, e of the MARC field 100 are standardized as `Bornhöhe, Eduard (1862-1923) [autor]`.

Subfields i, t or both can be used to indicate the precise relationship of the person to the record (e.g. `Sisaldab: Kunnas, Leo (1967-): "Kustumatu valguse maailm"`)

## publisher

*Name of the publisher, distributor, etc.*

**MARC source**: 260\$b, 264\$b

In 2022, the cataloguing practices at the NLE changed and information previously noted on the MARC field 260 began to be noted on 264. As a result, the publisher field is concatenated from the respective subfields of these MARC fields.

## title

*Title of the publication*

**MARC source**: 245$a


## title_remainder

*Subtitle or continuation of the title field*

**MARC source**: x

asd

## col

*desc*

**MARC source**: x

asd

## col

*desc*

**MARC source**: x

asd

## col

*desc*

**MARC source**: x

asd

## col

*desc*

**MARC source**: x

asd
