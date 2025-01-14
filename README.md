# ENB Curator

A modular pipeline for transforming and curating the **Estonian National Bibliography (ENB)** records into a structured, analysis-ready dataset. This project supports large-scale, reproducible data processing to facilitate research into Estonian bibliographic data.

> **Authors**: Krister Kruusmaa, Peeter Tinits, Laura Nemvalts  
> **Institution**: National Library of Estonia  
> **License**: [MIT](https://mit-license.org/)

Related publication: [TBA]

#### If you just want to download the datasets:
- **Books**: [https://doi.org/10.5281/zenodo.14083327](https://doi.org/10.5281/zenodo.14083327)
- **Persons**: [https://doi.org/10.5281/zenodo.14094584](https://doi.org/10.5281/zenodo.14094584)

## Overview

The ENB Curator pipeline is designed for data transformation in three key stages:
1. **Harvesting** - Retrieves MARC21XML records via OAI-PMH directly from the National Library of Estonia.
2. **Conversion** - Converts MARC records to a tabular format, ensuring readability and retaining relationships between fields.
3. **Curation** - Applies cleaning, harmonization, and enrichment operations to produce a coherent dataset suitable for data analysis.

This pipeline focuses on ensuring reproducibility, modularity, and scalability. It can be adapted for different bibliographic datasets or extended with new processing modules as needed.

### Requirements
- Python 3.8+ (3.9.12 recommended)

### Installation and usage
1. Clone the repository:
   ```
   git clone https://github.com/RaRa-digiLab/enb-curator.git
   cd enb-curator
   ```

2. Install required packages:
   ```
   pip install -r requirements.txt
   ```

2. Run the pipeline:
   ```
   python main.py "erb_all_books" # or
   python main.py "nle_persons"
   ```

3. Collect the curated, up-to-date dataset from `./data/curated`
