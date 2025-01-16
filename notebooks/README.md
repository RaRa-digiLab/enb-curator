# Notebooks

Included in this folder are stand-alone explorations of the data and modular additions to the pipeline.

## Overview

- [main_descriptives.Rmd](main_descriptives.Rmd) - A live summary file that generates the [../reports/main_descriptives.pdf](main_descriptives.pdf) report.

## Processing

- [harmonize_places.Rmd](harmonize_places.Rmd) - The pipeline to harmonize the place names relying on [../config/places/places_rules.tsv](rules) and geographical information. It generates the [../config/places/places_harmonized.tsv](mappings) between place name variants and harmonized names in the pipeline config.
- [harmonize_publishers.Rmd](harmonize_publishers.Rmd) - The pipeline to harmonize the publisher names relying on [../config/publishers/publisher_harmonize_rules.tsv](rules) and [../config/places/places_harmonized.tsv](harmonized place names). It generates the [../config/publishers/publisher_harmonization_mapping.json](mappings between publisher name variants and harmonized names).
- [adding_gender.ipynb](adding_gender.ipynb) - The pipeline to add gender to person ids in the dataset.
- [linking_viaf_and_wikidata.ipynb](linking_viaf_and_wikidata.ipynb) - The pipeline to link person ids with ids in viaf and wikidata.
- [experiments/filter-and-clean.ipynb](experiments/filter-and-clean.ipynb) - Experiments in filtering and cleaning various bibliographic fields where the relevant parts have already been integrated into the main pipeline.

## Case studies

There are three case studies reported in the associated article. These explorations are provided each in a separate notebook that generates the figures from the curated data.

- [case_study_1_languages.Rmd](case_study_1_languages.Rmd)
- [case_study_2_estonian_diaspora.ipynb](case_study_2_estonian_diaspora.ipynb)          
- [case_study_3_historiography.ipynb](case_study_3_historiography.ipynb) 

## Experiments

The experiments folder contains a notebook that was used to test different (rule-based) cleaning methods for the `books` dataset.

