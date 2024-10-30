import json
from tqdm import tqdm
from pathlib import Path
import requests
from lxml import etree

# Path to the current script
current_script_path = Path(__file__)
# Path to the project root
project_root = current_script_path.parent.parent.parent
# Path to write directory
write_data_path = project_root / "data" / "raw"
# Path to the collections.json file in the config directory
collections_file_path = project_root / "config" / "collections.json"

with open(collections_file_path, "r", encoding="utf8") as f:
    collections = json.load(f)

# Define and register namespaces
ns = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "marc": "http://www.loc.gov/MARC21/slim",
}
for prefix, uri in ns.items():
    etree.register_namespace(prefix, uri)


def request_records(collection_URL=None, token=None):
    """
    Sends a request to the OAI-PMH endpoint and retrieves the ListRecords element and resumptionToken.
    """
    # Determine the URL to use
    if token is not None and collection_URL is None:
        URL = f"https://data.digar.ee/repox/OAIHandler?verb=ListRecords&resumptionToken={token}"
    elif collection_URL is not None and token is None:
        URL = collection_URL
    else:
        raise AttributeError("Must provide either a resumptionToken or a collection URL")

    response = requests.get(URL)
    root = etree.fromstring(response.content)

    # Get the ListRecords element
    ListRecords = root.find("./oai:ListRecords", namespaces=ns)
    if ListRecords is not None:
        # Extract the resumptionToken from the response
        resumptionToken_element = ListRecords.find("./oai:resumptionToken", namespaces=ns)
        resumptionToken = resumptionToken_element.text if resumptionToken_element is not None else None
    else:
        resumptionToken = None

    return root, ListRecords, resumptionToken


def harvest_and_write_records(URL, savepath, verbose=True):
    """
    Harvests records from the OAI-PMH endpoint and writes them directly to the XML file without storing all records in memory.
    """
    # Open the file and write the start of the XML document
    with open(savepath, "w", encoding="utf8") as f:
        # Initial request
        root, ListRecords, resumptionToken = request_records(collection_URL=URL)

        # Get responseDate and request elements for the XML header
        responseDate = root.find("./oai:responseDate", namespaces=ns)
        request_element = root.find("./oai:request", namespaces=ns)
        request_metadata = {
            "responseDate": responseDate,
            "request": request_element,
        }

        # Write the XML header
        f.write(write_start_of_string(request_metadata))
        f.write("<ListRecords>\n")

        # Initialize progress bar and token
        if ListRecords is not None:
            records = ListRecords.findall("./oai:record", namespaces=ns)
            token = resumptionToken
            if token is not None:
                token_parts = token.strip(":").split(":")
                if len(token_parts) >= 5:
                    collection_size = int(token_parts[4])
                    total_records = collection_size
                else:
                    total_records = None
            else:
                total_records = len(records)
        else:
            records = []
            token = None
            total_records = 0

        if verbose:
            if total_records is not None:
                progress_bar = tqdm(total=total_records)
                progress_bar.update(len(records))
            else:
                progress_bar = tqdm()
        else:
            progress_bar = None

        # Write the initial records
        for record in records:
            entry_as_string = etree.tostring(
                record,
                encoding="utf8",
                pretty_print=True,
            ).decode()
            f.write(entry_as_string)
            if progress_bar:
                progress_bar.update(1)

        # Harvest and write subsequent records
        while token is not None:
            # Request the next batch of records using the resumptionToken
            root, ListRecords, resumptionToken = request_records(token=token)
            if ListRecords is not None:
                records = ListRecords.findall("./oai:record", namespaces=ns)
                for record in records:
                    entry_as_string = etree.tostring(
                        record,
                        encoding="utf8",
                        pretty_print=True,
                    ).decode()
                    f.write(entry_as_string)
                    if progress_bar:
                        progress_bar.update(1)
                # Update the token with the new resumptionToken from the response
                token = resumptionToken
            else:
                token = None

        # Close the XML document
        f.write("</ListRecords>\n")
        f.write("</OAI-PMH>")

        if progress_bar:
            progress_bar.close()


def write_start_of_string(metadata: dict) -> str:
    """
    Generates and returns the beginning of an OAI-PMH XML string based on the metadata dictionary passed as argument.
    """
    xml_string = """<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">\n"""
    xml_string += etree.tostring(metadata["responseDate"], encoding="utf8", pretty_print=True).decode()
    xml_string += etree.tostring(metadata["request"], encoding="utf8", pretty_print=True).decode()
    return xml_string


def harvest_oai(key: str, savepath: str) -> None:
    """
    Harvests metadata records from an OAI-PMH endpoint for a given collection and writes them to a file.
    """
    URL = collections[key]["OAI-PMH"]
    harvest_and_write_records(URL=URL, savepath=savepath)


if __name__ == "__main__":
    import sys

    key = sys.argv[1]
    print(f"Harvesting {collections[key]['title']}")
    harvest_oai(key=key, savepath=f"{write_data_path}/{key}.xml")