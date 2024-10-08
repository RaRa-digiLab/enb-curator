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


def update_cursor(token: str, step: int):
    """
    Update a given resumptionToken with the specified step size.
    """
    if not token:
        return None
    token_parts = token.strip(":").split(":")
    if len(token_parts) >= 5:
        token_id, collection, metadata_prefix, cursor, collection_size = token_parts[:5]
        new_cursor = str(int(cursor) + step)
        if int(new_cursor) >= int(collection_size):  # reached the last batch
            return None
        else:
            new_token = ":".join([token_id, collection, metadata_prefix, new_cursor, collection_size]) + ":"
            return new_token
    else:
        return None


def request_records(collection_URL=None, token=None):
    """
    Given an OAI-PMH collection URL or a resumptionToken, sends a request to the endpoint and retrieves the corresponding
    ListRecords element. If an initial request is made, returns both the records and the resumptionToken, as well as the
    request metadata.
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

    if token is None:
        # Initial request
        responseDate = root.find("./oai:responseDate", namespaces=ns)
        request_element = root.find("./oai:request", namespaces=ns)
        ListRecords = root.find("./oai:ListRecords", namespaces=ns)
        if ListRecords is not None:
            resumptionToken_element = ListRecords.find("./oai:resumptionToken", namespaces=ns)
            resumptionToken = resumptionToken_element.text if resumptionToken_element is not None else None
        else:
            resumptionToken = None
        # Save the request metadata
        request_metadata = {
            "responseDate": responseDate,
            "request": request_element,
            "resumptionToken": resumptionToken,
        }
        return ListRecords, request_metadata
    else:
        # Subsequent requests
        ListRecords = root.find("./oai:ListRecords", namespaces=ns)
        return ListRecords


def harvest_and_write_records(URL, savepath, verbose=True):
    """
    Harvests records from the OAI-PMH endpoint and writes them directly to the XML file without storing all records in memory.
    """
    # Open the file and write the start of the XML document
    with open(savepath, "w", encoding="utf8") as f:
        # Initial request
        ListRecords, request_metadata = request_records(collection_URL=URL)

        # Write the XML header
        f.write(write_start_of_string(request_metadata))
        f.write("<ListRecords>\n")

        # Initialize progress bar and token
        if ListRecords is not None:
            records = ListRecords.findall("./oai:record", namespaces=ns)
            total_records = None
            token = request_metadata["resumptionToken"]
            if token is not None:
                token_parts = token.strip(":").split(":")
                if len(token_parts) >= 5:
                    next_cursor = int(token_parts[3])
                    collection_size = int(token_parts[4])
                    cursor = next_cursor - len(records)  # Current cursor position
                    total_records = collection_size
                else:
                    cursor = 0
                    total_records = None
            else:
                cursor = 0
                total_records = len(records)
        else:
            records = []
            token = None
            cursor = 0
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
            # Update the token
            token = update_cursor(token, step=len(records))
            if token is None:
                break
            ListRecords = request_records(token=token)
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