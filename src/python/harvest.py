import json
from tqdm import tqdm
from pathlib import Path
import requests
from lxml import etree
from lxml.etree import ElementTree as ET

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



ns = {"oai": "http://www.openarchives.org/OAI/2.0/",
      "xsi": "http://www.w3.org/2001/XMLSchema-instance"}
for key, value in ns.items():
    etree.register_namespace(key, value)


def update_cursor(token: str, step: int):
    """
    Update a given resumptionToken with the specified step size.

    Parameters:
    -----------
    token : str
        The resumptionToken to update, in the format `token_id:collection:metadata_prefix:cursor:collection_size`
    step : int
        The number of records to advance the cursor by.

    Returns:
    --------
    str or None
        The updated resumptionToken, in the same format as the input token, or None if the end of the collection has been reached.
    """
    token_id, collection, metadata_prefix, cursor, collection_size = token.strip(":").split(":")
    new_cursor = str(int(cursor) + step)
    if int(new_cursor) >= int(collection_size): # reached the last batch
        return None
    else:
        return ":".join([token_id, collection, metadata_prefix, new_cursor, collection_size, ":"])


def request_records(collection_URL=None, token=None):
    """
    Given an OAI-PMH collection URL or a resumptionToken, sends a request to the endpoint and retrieves the corresponding
    ListRecords element. If an initial request is made, returns both the records and the resumptionToken, as well as the
    request metadata.

    Parameters:
    - collection_URL (str): the OAI-PMH collection URL to query.
    - token (str): the resumptionToken to use to continue a previous query.

    Returns:
    - (lxml.etree.ElementTree): the ListRecords element corresponding to the requested records.
    - (dict): the request metadata, including the responseDate, request, and resumptionToken (if applicable).
    """
    # if we don't have a resumptionToken yet, request the first batch; else use the token.
    if token is not None and collection_URL is None:
        URL = f"https://data.digar.ee/repox/OAIHandler?verb=ListRecords&resumptionToken={token}"
    elif collection_URL is not None and token is None:
        URL = collection_URL
    else:
        raise AttributeError("Must provide either a resumptionToken or a collection URL (see harvester.collections for details)")

    response = requests.get(URL)
    tree = ET(etree.fromstring(bytes(response.text, encoding="utf8")))
    root = tree.getroot()
    responseDate, request, ListRecords = root.getchildren()

    # in the case of an initial request, return both the records, resumptionToken and the request metadata
    if token is None:
        try:
            resumptionToken = root.find("./{*}ListRecords/{*}resumptionToken").text
        except AttributeError:
            resumptionToken = None
        # save the request metadata and the token as a class attribute
        request_metadata = {"responseDate": responseDate,
                            "request": request,
                            "resumptionToken": resumptionToken}
        return ListRecords, request_metadata
    # if we already have a resumptionToken, just get the records
    else:
        return ListRecords
    

def get_collection(URL):
    """
    Requests all records of a given OAI-PMH collection URL, and returns them as a list of xml ElementTree elements,
    together with the request metadata (e.g. the resumptionToken).

    Args:
        URL (str): The URL of the OAI-PMH collection.

    Returns:
        Tuple[List[xml.etree.ElementTree.Element], Dict[str, Any]]: A tuple containing two elements:
            - A list of xml.etree.ElementTree.Element objects, representing the records in the collection.
            - A dictionary containing the request metadata (e.g. the resumptionToken).

    Raises:
        AttributeError: If URL is None.
    """
    # initial request
    all_records = []
    ListRecords, request_metadata = request_records(collection_URL=URL)
    all_records += ListRecords[:-1]

    token = request_metadata["resumptionToken"]
    if token is not None:
        cursor_step, collection_size = [int(el) for el in token.split(":")[3:5]]
    else:   # token can be none in the case of a small collection that is returned in the initial request
        cursor_step, collection_size = 1000, len(ListRecords)

    progress_bar = tqdm(total=collection_size, initial=cursor_step)
    while token is not None: # continue requesting until there is no more resumptionToken, i.e. the end of the collection is reached
        ListRecords = request_records(token=token)
        all_records += ListRecords[:-1] # (leave out the last element, the resumptionToken)
        token = update_cursor(token, step=cursor_step) # update the cursor
        progress_bar.update(len(ListRecords)-1)
    progress_bar.close()

    return all_records, request_metadata


def write_start_of_string(metadata: dict) -> str:
    """
    Generates and returns the beginning of an OAI-PMH XML string based on the metadata dictionary passed as argument.

    Args:
    - metadata (dict): a dictionary containing the metadata returned by an OAI-PMH repository, including the responseDate and request elements.

    Returns:
    - str: the XML string representing the start of the OAI-PMH response, including the responseDate and request elements.
    """
    xml_string = """<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">"""
    xml_string += etree.tostring(metadata["responseDate"],
                                 encoding="utf8",
                                 pretty_print=True).decode()
    xml_string += etree.tostring(metadata["request"],
                                 encoding="utf8",
                                 pretty_print=True).decode()       
    return xml_string


def write_records(ListRecords: list, metadata: dict, savepath: str) -> None:
    """
    Writes OAI-PMH XML records to a file.

    Args:
    - ListRecords: list of OAI-PMH XML records, as returned by get_collection() function
    - metadata: dictionary with response metadata, as returned by get_collection() function
    - savepath: string indicating the file path where the records will be saved
    
    Returns: None
    """
    with open(savepath, "a", encoding="utf8") as f: 
        f.write(write_start_of_string(metadata))
        f.write("<ListRecords>")
        for entry in ListRecords:
            entry_as_xml_tree = ET(entry)
            entry_as_string = etree.tostring(entry_as_xml_tree,
                                            encoding="utf8",
                                            pretty_print=True,
                                            ).decode()
            f.write(entry_as_string)
        f.write("</ListRecords>")
        f.write("</OAI-PMH>")


def harvest_oai(key: str, savepath: str) -> None:
    """
    Harvests metadata records from an OAI-PMH endpoint for a given collection and writes them to a file.

    Args:
        collection_key (str): The key of the collection to harvest. See harvester.collections for the available keys, titles and URLs.
        savepath (str): The path to the file where the harvested records will be saved.

    Returns:
        None.

    Raises:
        ValueError: If the specified collection name is not found in the `collections` dictionary.
        Exception: If an error occurs during the harvesting process.

    Example:
        To harvest metadata records from the "ERB - Estonian books" OAI-PMH collection and save them to a file called "records.xml",
        you can call the function as follows:

        >>> harvest_oai("erb_books", "ERB_estonian_books.xml")

    """
    URL = collections[key]["OAI-PMH"]
    ListRecords, request_metadata = get_collection(URL=URL)
    write_records(ListRecords=ListRecords,
                  metadata=request_metadata,
                  savepath=savepath)



if __name__ == "__main__":

    import sys

    key = sys.argv[1]
    print(f"Harvesting {collections[key]['title']}")
    harvest_oai(key=key, savepath=f"{write_data_path}/{key}.xml")