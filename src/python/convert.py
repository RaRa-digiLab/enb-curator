from pymarc import parse_xml
from pymarc.record import Record
from pymarc.marcxml import XmlHandler, MARC_XML_NS
from lxml import etree
import pandas as pd
from pathlib import Path
import json
import re
import io
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor

# Path to the current script
current_script_path = Path(__file__)
# Path to the project root
project_root = current_script_path.parent.parent.parent
# Path to read directory
read_data_path = project_root / "data" / "raw"
# Path to write directory
write_data_path = project_root / "data" / "converted"
# Path to the collections.json file in the config directory
column_names_file_path = project_root / "config" / "marc_columns_dict.json"

with open(column_names_file_path, "r", encoding="utf8") as f:
    marc_columns_dict = json.load(f)


class MyContentHandler(XmlHandler):
    
    def endElementNS(self, name, qname):
        """End element NS."""
        if self._strict and name[0] != MARC_XML_NS:
            return

        element = name[1]
        if self.normalize_form is not None:
            text = unicodedata.normalize(self.normalize_form, "".join(self._text))
        else:
            text = "".join(self._text)

        if element == "record":
            self.process_record(self._record)
            self._record = None
        elif element == "leader":
            self._record.leader = text
        elif element == "controlfield":
            self._field.data = text
            self._record.add_field(self._field)
            self._field = None
        elif element == "datafield":
            self._record.add_field(self._field)
            self._field = None
        elif element == "subfield":
            # added exception to the parent class to ignore a few coding errors in the RaRa data
            try:
                self._field.subfields.append(self._subfield_code)
                self._field.subfields.append(text)
                self._subfield_code = None
            except AttributeError:
                pass

        self._text = []


class MARCrecordParser():
    """
    A class to parse a MARC record and extract the fields and subfields.

    Args:
        record (Record): A MARC record.

    Attributes:
        fields (list): A list of fields in the MARC record.
        marc_paths (dict): A dictionary of the paths and values of the fields in the MARC record.
        duplicate_field_sep (str): A separator for duplicate fields.
        return_control_fields (bool): Whether or not to return control fields.

    Methods:
        join_subfields_list(subfields_list):
            Join a list of subfields into a single dictionary.

        clean_person_dates(dates):
            Clean up the dates in person info (e.g. "(1855-1900)").

        handle_person_subfields(subfields):
            Combine the subfields of persons (name, dates, role etc.) into one string.

        clean_field(value):
            Simple preprocessing to remove trailing punctuation, etc.

        append_field(field, value):
            Append a field and its value to the marc_paths dictionary.

        sort_marc_paths():
            Sort the marc_paths dictionary.

        parse():
            Parse the fields in the MARC record and return a dictionary of the paths and values of the fields.
    """

    def __init__(self, record: Record):
        self.fields = record.as_dict()["fields"]
        self.marc_paths = {}
        self.duplicate_field_sep = "; "
        self.return_control_fields = True

    def join_subfields_list(self, subfields_list: list):
        subfields = {}
        for d in subfields_list:
            subfields.update(d)
        return subfields

    def clean_person_dates(self, dates: str):
        dates = dates.rstrip(".,: ")
        if dates.endswith(")"):
            if dates.startswith("("):
                pass
            else:
                dates.strip(")")
        return dates

    def handle_person_subfields(self, subfields: dict):
        name = None
        dates = None
        role = None
        info = None
        title = None

        if "a" in subfields.keys():
            name = subfields["a"].rstrip(" ,:.;")
        if "d" in subfields.keys():
            dates = " (" + self.clean_person_dates(subfields["d"]) + ")"
        if "e" in subfields.keys():
            role = " [" + subfields["e"].rstrip(" ,:.;") + "]"
        if "i" in subfields.keys():
            info = subfields["i"].rstrip(" ,:.;") + ": "
        if "t" in subfields.keys():
            title = ': "' + subfields["t"].rstrip(" ,:.;") + '"'
        
        return f'{info or ""}{name or ""}{dates or ""}{role or ""}{title or ""}'
    
    def handle_corporate_subfields(self, subfields: dict):
        corporate_unit = None
        corporate_sub_unit = None
        if "a" in subfields.keys():
            corporate_unit = subfields["a"].rstrip(" ,:.;")
        if "b" in subfields.keys():
            corporate_sub_unit = " [" + subfields["b"].rstrip(" ,:.;") + "]"

        return f'{corporate_unit or ""}{corporate_sub_unit or ""}'
    
    def handle_keyword_subfields(self, subfields: dict):
        keyword = None
        keyword_id = None
        if "a" in subfields.keys():
            keyword = subfields["a"].strip(".")
            if "0" in subfields.keys():
                keyword_link = subfields["0"]
                keyword_id = keyword_link.split("id/")[-1].strip(".")  # specific to EMS links in the ENB

        return f'{keyword} [{keyword_id or ""}]'

    def clean_field(self, value):
        if value.startswith("http"):
            return value.rstrip(".")
        else:
            value = value.rstrip(" ,:.;/")
            if len(value) > 0:
                for opening, closing in zip(["(", "["], [")", "]"]):
                    if value[-1] == closing and opening not in value:
                        value = value.rstrip(closing)
                    elif value[0] == opening and closing not in value:
                        value = value.lstrip(opening)
                    elif value[0] == opening and value[-1] == closing:
                        value = value.lstrip(opening).strip(closing)
            return value

    def append_field(self, field, value):
        if self.return_control_fields == False and field in ["006", "007"]:
            pass
        else:
            try:
                value = self.clean_field(value)
            except IndexError:
                pass
            if field not in self.marc_paths.keys():
                self.marc_paths[field] = value
            else:
                self.marc_paths[field] += self.duplicate_field_sep + value

    def sort_marc_paths(self):
        sorted_keys = sorted(self.marc_paths.keys(), key=lambda x: int(x.split("$")[0]))
        self.marc_paths = {key: self.marc_paths[key] for key in sorted_keys}

    def parse(self):
        for field in self.fields:
            path, value = next(iter(field.items()))
            if path[0] == "9":
                # skip these fields - not needed in ENB
                pass
            else:
                if isinstance(value, dict):
                    subfields = self.join_subfields_list(value["subfields"])

                    if path in ["100", "600", "700"]:
                        # person fields exception
                        person_string = self.handle_person_subfields(subfields)
                        self.append_field(path, person_string)

                    elif path in ["710"]:
                        # corporate field exception
                        corporate_string = self.handle_corporate_subfields(subfields)
                        self.append_field(path, corporate_string)

                    elif path in ["650", "651", "655"]:
                        # keyword fields exception
                        keyword_string = self.handle_keyword_subfields(subfields)
                        self.append_field(path, keyword_string)                      

                    else:
                        # standard approach for all other fields
                        for key, subval in subfields.items():
                            subpath = path + "$" + key
                            self.append_field(subpath, subval)

                elif isinstance(value, str):
                    # control fields
                    if self.return_control_fields == False:
                        pass
                    else:
                        self.append_field(path, value)

        self.sort_marc_paths()
        return self.marc_paths
    

class DCrecordParser():
    """
    A class to parse Dublin Core metadata from an EDM record.

    Attributes:
        namespaces (dict): A dictionary containing the XML namespaces used in the EDM record.
        fields (etree.ElementIterable): An iterator containing the Dublin Core fields in the EDM record.
        dc_fields (dict): A dictionary containing the parsed Dublin Core fields from the EDM record.
        sep (str): A string used to join multiple field values.

    Methods:
        extract_year(date):
            Extracts a valid year from a datetime string.

        parse():
            Parses the Dublin Core fields in the EDM record and returns them as a dictionary.
    """


    def __init__(self, record: etree._ElementTree):
        self.namespaces = {"xsi": "http://www.w3.org/2001/XMLSchema-instance",
                           "oai": "http://www.openarchives.org/OAI/2.0/",
                           "marc": "http://www.loc.gov/MARC21/slim",
                           "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                           "edm": "http://www.europeana.eu/schemas/edm/",
                           "dc" : "http://purl.org/dc/elements/1.1/"}
        self.fields = record.iterfind("./oai:metadata/rdf:RDF/edm:ProvidedCHO/dc:*",
                                      namespaces=self.namespaces)
        self.dc_fields = {}
        self.sep = "; "

    def extract_year(self, date):
        """
        Cleans a datetime string to find a valid year.
        """
        if len(date) == 4 and date.isnumeric():
            if int(date) > 1500 and int(date) < 2024:
                return int(date)
            else:
                return None
        patterns = [re.compile("(^([\D\s]+)(\d{4})([\D\s]*)$)|(^([\D\s]*)(\d{4})([\D\s]+)$)"),
                    re.compile("^\d{4}-\d{2}-\d{2}$"),
                    re.compile("^\d{2}-\d{2}-\d{4}$"),
                    re.compile("^\d{4}-\d{2}$")]
        for pattern in patterns:
            if re.match(pattern, date):
                date = re.findall("\d{4}", date)[0]
        if len(date) == 4:
            try:
                date = int(date)
                if date > 1500 and date < 2024:
                    return date
            except ValueError:
                return None
        else:
            return None
    
    def parse(self):
        """Converts a single EDM record to a dictionary"""
        for f in self.fields:
            if f is not None:
                tag = f.tag.rsplit("}", 1)[1]
                lang = f.attrib.get("{http://www.w3.org/XML/1998/namespace}lang")
                text = f.text

                if text is not None:
                    if tag == "identifier":
                        if ":isbn:" in text:
                            tag = "isbn"
                        elif "www.ester.ee" in text:
                            tag = "ester_url"
                        elif "www.digar.ee" in text:
                            tag = "digar_url"
                        else:
                            tag = "other_identifier"

                    if tag == "date":
                        self.dc_fields["year"] = self.extract_year(text)        
                    if lang is not None:
                        tag = tag + "_" + lang 
                    if tag in self.dc_fields.keys():
                        self.dc_fields[tag] += self.sep + text
                    else:
                        self.dc_fields[tag] = text

        return self.dc_fields
    

def register_namespaces():
    for key, value in get_namespaces().items():
        etree.register_namespace(key, value)


def read_marc_records(filepath):
    if filepath[-4:] != ".xml":
        raise ValueError("Filepath must be in XML format")
    else:
        handler = MyContentHandler()
        with open(filepath, "r", encoding="utf8") as f:
            parse_xml(f, handler=handler)
        marc_records = handler.records
        marc_records = [record for record in marc_records if record is not None]
        return marc_records
    

def read_edm_records(source):
    """Parses the records of an EDM tree and returns the record objects.
    Input: filepath or lxml.etree._ElementTree object
    Output: list"""

    if isinstance(source, str):
        if source.lower().endswith(".xml"):
            tree = etree.parse(source)
        else:
            raise ValueError("Invalid path to file. Must be in .xml format.")
    elif isinstance(source, etree._ElementTree):
        tree = source
    else:
        raise ValueError("Source must be either path to XML file or lxml.etree._ElementTree")

    register_namespaces()
    root = tree.getroot()
    records = root.findall("./oai:ListRecords/oai:record", namespaces=get_namespaces())
    return records


def parse_marcxml_record(record_xml):
    handler = MyContentHandler()
    record_xml_file = io.StringIO(record_xml)  # Convert the XML string to a file-like object
    parse_xml(record_xml_file, handler=handler)
    if handler.records:
        record = handler.records[0]
        return MARCrecordParser(record).parse()
    return {}


def read_marc_records_stream(filepath):
    context = etree.iterparse(filepath, events=("end",), tag="{http://www.loc.gov/MARC21/slim}record")
    for event, elem in context:
        record_xml = etree.tostring(elem, encoding="unicode")
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
        yield record_xml


def marc_to_dataframe(records_stream, columns_dict, min_filled_ratio, rename_columns):
    with ProcessPoolExecutor() as executor:
        results = list(tqdm(executor.map(parse_marcxml_record, records_stream), total=13288))  # Adjust the total if needed
    df = pd.DataFrame.from_records(results)
    column_population = df.notna().sum() / len(df)  # how populated the columns are
    df = df[column_population.loc[column_population > min_filled_ratio].index].copy()
    if rename_columns:
        df.columns = [columns_dict[col] if col in columns_dict else col for col in df.columns]
    return df


def get_namespaces():
    return {"xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "oai": "http://www.openarchives.org/OAI/2.0/",
            "marc": "http://www.loc.gov/MARC21/slim",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "edm": "http://www.europeana.eu/schemas/edm/",
            "dc" : "http://purl.org/dc/elements/1.1/"}


def detect_format(filepath):
    """Detects whether a parsed XML file is in OAI-PMH or EDM format"""
    ns = get_namespaces()
    context = etree.iterparse(filepath, events=("start",), tag=["{http://www.loc.gov/MARC21/slim}record", 
                                                                "{http://www.europeana.eu/schemas/edm/}ProvidedCHO"])
    
    for event, elem in context:
        if elem.tag == "{http://www.loc.gov/MARC21/slim}record":
            print("Detected MARC format. Proceeding to convert.")
            return "marc"
        elif elem.tag == "{http://www.europeana.eu/schemas/edm/}ProvidedCHO":
            print("Detected EDM format. Proceeding to convert.")
            return "edm"

    raise ValueError("Cannot determine data format. The OAI-PMH ListRecords response must be made up of either EDM or MARC21XML records.")


def oai_to_dataframe(filepath: str, min_filled_ratio: float=0.1, rename_columns: bool=False) -> pd.DataFrame:
    """
    Converts an OAI-PMH file to a pandas DataFrame.

    Parameters:
    -----------
    filepath : str
        The path to the input OAI-PMH file.
    min_filled_ratio : float, optional (default=0.1)
        The threshold value used for filtering out empty columns in the output DataFrame
        (only used when the input file is in MARCXML format).
    rename_columns : bool, optional (default=True)
        In the case of MARC data, whether to replace the MARC field names with more informative ones
        (these unofficial field names are hand-crafted for about 200 different fields).

    Returns:
    --------
    pandas.DataFrame
        A DataFrame containing the extracted metadata, with columns corresponding to
        the Dublin Core (DC) elements or MARC fields.

    Raises:
    -------
    ValueError
        If the input file is not in a supported format.

    Examples:
    ---------
    >>> df = oai_to_dataframe("my_file.xml")
    >>> df.head()

    """

    format = detect_format(filepath)
    if format == "edm":
        with open(filepath, "r", encoding="utf8") as f:
            tree = etree.parse(f)
            xml_records = read_edm_records(tree)
            dc_records = (DCrecordParser(record).parse() for record in xml_records)
            df = pd.DataFrame.from_records(dc_records).convert_dtypes()
        return df
    elif format == "marc":
        records_stream = read_marc_records_stream(filepath)
        df = marc_to_dataframe(records_stream=records_stream,
                               columns_dict=marc_columns_dict,
                               min_filled_ratio=min_filled_ratio,
                               rename_columns=rename_columns).convert_dtypes()
        return df
    else:
        raise ValueError("Unsupported format")
    

def oai_to_dict(filepath: str):
    """
    Parses an OAI-PMH XML file at `filepath` and returns a dictionary
    containing the records as either EDM Dublin Core or MARC21XML.

    Args:
        filepath (str): The path to the OAI-PMH XML file to parse.

    Returns:
        dict: A dictionary containing the parsed records. The keys of the dictionary
        are string representations of integers, starting from 0 and increasing by 1
        for each record. The values of the dictionary are the records themselves,
        represented as dictionaries.

    Raises:
        TypeError: If the format of the XML file at `filepath` is not EDM or MARC21XML.
    """
    f = open(filepath, "r", encoding="utf8")
    tree = etree.parse(f)
    format = detect_format(tree)
    if format == "edm":
        xml_records = read_edm_records(tree)
        f.close()
        json_records = {"records": {}}
        for i, record in enumerate(xml_records):
            json_records["records"][str(i)] = DCrecordParser(record).parse()
        return json_records
    elif format == "marc":
        f.close()
        marc_records = read_marc_records(filepath)
        json_records = {"records": {}}
        for i, record in enumerate(marc_records):
            json_records["records"][str(i)] = record.as_dict()
        return json_records                   
    else:
        raise TypeError("The filepath provided does not seem to contain EDM Dublin Core or MARC21XML records.")


def oai_to_json(filepath: str, json_output_path: str):
    """
    Converts an OAI-PMH XML file containing EDM Dublin Core or MARC21XML records to a JSON file.

    Args:
        filepath (str): The path to the input OAI-PMH XML file.
        json_output_path (str): The path where the output JSON file will be saved.

    Returns:
        None

    Raises:
        TypeError: If the OAI-PMH XML file does not contain EDM Dublin Core or MARC21XML records.
    """
    json_records = oai_to_dict(filepath)
    with open(json_output_path, "w", encoding="utf8") as f:
        json.dump(json_records, f)


if __name__ == "__main__":

    import sys
    key = sys.argv[1]
    min_filled_ratio = float(sys.argv[2])

    print(f"Converting {key} to dataframe")
    df = oai_to_dataframe(f"{read_data_path}/{key}.xml", min_filled_ratio=min_filled_ratio, rename_columns=False)
    df.to_parquet(f"{write_data_path}/{key}.parquet")