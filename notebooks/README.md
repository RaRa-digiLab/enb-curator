# Notebooks

Included in this folder are stand-alone explorations of the data and modular additions to the pipeline.

## Overview

- [`main_descriptives.Rmd`](main_descriptives.Rmd) - A live summary file that generates the [`main_descriptives.pdf`](../reports/main_descriptives.pdf) report.

## Processing

- [`harmonize_places.Rmd`](harmonize_places.Rmd) - The pipeline to harmonize the place names relying on [rules](../config/places/places_rules.tsv) and geographical information. It generates the [mappings](../config/places/places_harmonized.tsv) between place name variants and harmonized names in the pipeline config.
- [`harmonize_publishers.Rmd`](harmonize_publishers.Rmd) - The pipeline to harmonize the publisher names relying on [rules](../config/publishers/publisher_harmonize_rules.tsv) and [harmonized place names](../config/places/places_harmonized.tsv). It generates the [mappings](../config/publishers/publisher_harmonization_mapping.tsv) between publisher name variants and harmonized names).
- [`publisher_similarity_groups.ipynb`](publisher_similarity_groups.ipynb) - The pipeline for retrieving embeddings for harmonized publisher names and using them to create vector-based similarity [clusters](../config/publishers/publisher_similarity_groups.tsv).
- [`adding_gender.ipynb`](adding_gender.ipynb) - The pipeline to add gender to person ids in the dataset.
- [`linking_viaf_and_wikidata.ipynb`](linking_viaf_and_wikidata.ipynb) - The pipeline to link person ids with ids in viaf and wikidata.

## Case studies

There are three case studies reported in the associated article. These explorations are provided each in a separate notebook that generates the figures from the curated data.

- [`case_study_1_languages.Rmd`](case_study_1_languages.Rmd)
- [`case_study_2_estonian_diaspora.ipynb`](case_study_2_estonian_diaspora.ipynb)          
- [`case_study_3_historiography.ipynb`](case_study_3_historiography.ipynb) 

## Experiments

- [`experiments/filter-and-clean.ipynb`](experiments/filter-and-clean.ipynb) - Experiments in filtering and cleaning various bibliographic fields where the relevant parts have already been integrated into the main pipeline.

