# Config

The folder includes files that are used as input in the main pipeline.

## Geoinfo

The geoinfo files used in place name harmonization are stored here for the scripts. They are not shared in the repository. Contact us if you want to use these.

## Persons

- [`persons_gender.tsv`](persons/persons_gender.tsv) - The mappings between person ids and gender.
- [`persons_id_links.tsv`](persons/persons_id_links.tsv) - The mappings between person ids in the bibliography, VIAF and Wikidata.

## Places

- [`places_coordinates.tsv`](places/places_coordinates.tsv) - The mappings between harmonized place names and coordinates produced in place name harmonization.
- [`places_rules.tsv`](places/places_rules.tsv) - The ruleset relied on in harmonizing the place name variants.
- [`places_harmonized.tsv`](places/places_harmonized.tsv) - The mappings between the place name variants and harmonized place names produced in place name harmonization.
- [`resolved_manually_geo.tsv`](places/resolved_manually_geo.tsv) - Manually resolved input for geographic information.

## Publishers

- [`publisher_harmonization_mapping.json`](publishers/publisher_harmonization_mapping.json) - The mappings between publisher name variants and harmonized names.
- [`publisher_similarity_groups.tsv`](publishers/publisher_similarity_groups.tsv) - The mappings between harmonized names and publisher similarity groups based on text embeddings.
- [`publisher_harmonize_rules.tsv`](publishers/publisher_harmonize_rules.tsv) - The rules used in harmonizing the publisher name variants.

## Root

- [`collections.json`](collections.json) - List of parameters that can be used to access different parts of the Estonian National Bibliography.
- [`marc_columns_order.json`](marc_columns_order.json) - The order of columns in the curated dataset.
- [`marc_columns_to_keep.json`](marc_columns_to_keep.json) - The list of MARC fields to include in the pipeline.
- [`marc_columns_dict.json`](marc_columns_dict.json) - The mapping between numerical MARC fields and descriptive names used in the pipeline.
