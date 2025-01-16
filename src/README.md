# Source code

This is the source code for the pipeline. It is divided into three main scripts and a helper file, [`constants.py`](constants.py) that holds regex patterns.

1. [`harvest.py`](harvest.py) - The first stage of the pipeline which downloads the raw data in MARC21XML format from the National Library of Estonia's OAI-PMH endpoint. The script can also be used for other datasets than those belonging to the ENB (see [`../config/collections.json`](./config/collections.json) for all available datasets). Harvested files can be found in [`../data/raw/`](./data/raw).

2. [`convert.py`](convert.py) - Second stage of the pipeline which converts the raw MARC21XML into a tabular format (parquet). See the paper for discussion on the decisions made during this steps. When applying this code to other MARC21XML bibliographic datasets than the ENB, it is recommended to look into the methods of the `MARCrecordParser` class which contains some specifity to the Estonian data. This script is capable of processsing DublinCore data as well, although the ENB does not use this format.

3. [`curate.py`](curate.py) - Third and main stage of the pipeline which applies numerous cleaning, harmonization and enrichment functions to the tabular data. It also filters, renames and reorders the columns. Some of these functions make use of external data, in [`../config/`](./config). The files there can be changed for easy customization of the curation script.
