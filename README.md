# RaRa metadata handler
This is a tool for harvesting the public metadata resources of the **National Library of Estonia** and converting them to formats better suited for data analysis. The data is encoded in either Dublin Core or MARC21XML format, depending on the collection, and provided via an OAI-PMH endpoint. This package allows you to retrieve the collections more easily and convert them to common data formats.
For more information, see <https://digilab.rara.ee/>

### Installing the package and requirements
```
git clone https://github.com/krkryger/RaRa-metadata.git
pip install -r requirements.txt
```

### Harvesting metadata from the OAI-PMH endpoint
```
from harvester import collections, harvest_oai

# see the available datasets and choose one to download
print(collections)
>>> {...
     "nle_books": {
        "title": "DIGAR - books",
        "OAI-PMH": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=book&metadataPrefix=edm",
        "original_format": "Europeana Data Model"
    },
    "nle_journals": {
        "title": "DIGAR - journals",
        "OAI-PMH": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=journal&metadataPrefix=edm",
        "original_format": "Europeana Data Model"
    },
    "nle_maps": {
        "title": "DIGAR - maps",
        "OAI-PMH": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=map&metadataPrefix=edm",
        "original_format": "Europeana Data Model"
    },
     ...
    }
 
harvest_oai(key="nle_books",
            savepath="nle_books.xml")
```

### Converting downloaded files from XML to DataFrame/dict/JSON
```
from converter import oai_to_dataframe, oai_to_dict, oai_to_json

# convert to DataFrame and save as TSV
df = oai_to_dataframe(filepath="nle_books.xml")
df.to_csv("nle_books.tsv", sep="\t", encoding="utf8", index=False)

# convert to dictionary
records_as_dict = oai_to_dict(filepath="nle_books.xml")

# or save directly as JSON
oai_to_json(filepath="nle_books.xml",
            json_output_path="nle_books.json")
```

When converting MARC21XML files to a dataframe, the columns that are mostly empty will be dropped automatically. This can be modified with the ```marc_threshold``` parameter in the ```oai_to_dataframe``` function (the default value ```0.1``` means that columns with ≥ 90% NA values are dropped). Converting to dict or JSON keeps all fields.
