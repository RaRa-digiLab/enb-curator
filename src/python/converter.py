from pymarc import parse_xml
from pymarc.record import Record
from pymarc.marcxml import XmlHandler, MARC_XML_NS
from lxml import etree
import pandas as pd
import json
import re


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
        self.return_control_fields = False

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
        if "a" in subfields.keys():
            name = subfields["a"].rstrip(" ,:.;")
        else:
            name = None
        if "d" in subfields.keys():
            dates = " (" + self.clean_person_dates(subfields["d"]) + ")"
        else:
            dates = None
        if "e" in subfields.keys():
            role = " [" + subfields["e"].rstrip(" ,:.;") + "]"
        else:
            role = None
        if "i" in subfields.keys():
            info = subfields["i"].rstrip(" ,:.;") + ": "
        else:
            info = None
        if "t" in subfields.keys():
            title = ': "' + subfields["t"].rstrip(" ,:.;") + '"'
        else:
            title = None
        
        return f'{info or ""}{name or ""}{dates or ""}{role or ""}{title or ""}'

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
        if self.return_control_fields == False and field in ["006", "007", "008"]:
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
                pass
            else:
                if type(value) == dict:
                    subfields = self.join_subfields_list(value["subfields"])
                    if path in ["100", "600", "700"]:
                        person_string = self.handle_person_subfields(subfields)
                        self.append_field(path, person_string)
                    else:
                        for key, subval in subfields.items():
                            subpath = path + "$" + key
                            self.append_field(subpath, subval)
                elif type(value) == str:
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

    if type(source) == str:
        if source.lower().endswith(".xml"):
            tree = etree.parse(source)
        else:
            raise ValueError("Invalid path to file. Must be in .xml format.")
    elif type(source) == etree._ElementTree:
        tree = source
    else:
        raise ValueError("Source must be either path to XML file or lxml.etree._ElementTree")

    register_namespaces()
    root = tree.getroot()
    records = root.findall("./oai:ListRecords/oai:record", namespaces=get_namespaces())
    return records


def marc_to_dataframe(records, columns_dict, threshold, replace_columns):
    df = pd.DataFrame.from_records((MARCrecordParser(record).parse() for record in records))
    column_population = df.notna().sum() / len(df) # how populated the columns are
    df = df[column_population.loc[column_population > threshold].index].copy()
    if replace_columns:
        df.columns = [columns_dict[col] if col in columns_dict.keys() else col for col in df.columns]
    return df


def get_namespaces():
    return {"xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "oai": "http://www.openarchives.org/OAI/2.0/",
            "marc": "http://www.loc.gov/MARC21/slim",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "edm": "http://www.europeana.eu/schemas/edm/",
            "dc" : "http://purl.org/dc/elements/1.1/"}


def detect_format(tree):
    """Detects whether a parsed XML tree is in OAI-PMH or EDM format"""
    ns = get_namespaces()
    if tree.find("./oai:ListRecords/oai:record/oai:metadata/marc:*", namespaces=ns) is not None:
        print("Detected MARC format in OAI-PMH protocol. Proceeding to convert.")
        return "marc"
    elif tree.find("./marc:record", namespaces=ns) is not None:
        print("Detected MARC format without OAI-PMH protocol. Attempting to convert...")
        return "marc"
    elif tree.find("./oai:ListRecords/oai:record/oai:metadata/rdf:RDF/edm:*", namespaces=ns) is not None:
        print("Detected EDM format. Proceeding to convert.")
        return "edm"
    else:
        raise ValueError("Cannot determine data format. The OAI-PMH ListRecords response must be made up of either EDM or MARC21XML records.")


def oai_to_dataframe(filepath: str, marc_threshold: float=0.1, replace_columns: bool=True) -> pd.DataFrame:
    """
    Converts an OAI-PMH file to a pandas DataFrame.

    Parameters:
    -----------
    filepath : str
        The path to the input OAI-PMH file.
    marc_threshold : float, optional (default=0.1)
        The threshold value used for filtering out empty columns in the output DataFrame
        (only used when the input file is in MARCXML format).
    replace_columns : bool, optional (default=True)
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

    f = open(filepath, "r", encoding="utf8")
    tree = etree.parse(f)
    format = detect_format(tree)
    if format == "edm":
        xml_records = read_edm_records(tree)
        f.close()
        dc_records = (DCrecordParser(record).parse() for record in xml_records)
        df = pd.DataFrame.from_records(dc_records).convert_dtypes()
        return df
    elif format == "marc":
        f.close()
        marc_records = read_marc_records(filepath)
        df = marc_to_dataframe(records=marc_records,
                               columns_dict=marc_columns_dict,
                               threshold=marc_threshold,
                               replace_columns=replace_columns).convert_dtypes()
        return df
    

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


marc_columns_dict = {
    "001": "ID",
    "003": "control_nr_identifier",
    "008": "fixed_len_data",
    "020$a": "ISBN",
    "020$q": "ISBN_info",
    "022$a": "ISSN",
    "024$a": "other_standard_identifier",
    "028$a": "publisher_or_distributor_number",
    "028$b": "publisher_or_distributor_source",
    "034$a": "cartographic_scale",
    "034$b": "constant_ratio_linear_horizontal_scale",
    "034$d": "coordinates_western_max",
    "034$e": "coordinates_eastern_max",
    "034$f": "coordinates_northern_max",
    "034$g": "coordinates_southern_max",
    "035$a": "system_control_nr",
    "040$a": "cataloging_agency",
    "040$b": "cataloging_lang",
    "040$c": "transcribing_agency",
    "040$d": "modifying_agency",
    "040$e": "description_conventions",
    "041$a": "language",
    "041$b": "summary_or_abstract_language",
    "041$d": "singing_or_speaking_language",
    "041$f": "table_of_contents_language",
    "041$h": "language_original",
    "041$j": "subtitles_language",
    "043$a": "geographic_area_code",
    "072$a": "subject_category_code",
    "072$2": "category_source_code",
    "080$a": "UDC",
    "080$x": "UDC_subdivision",
    "080$2": "edition_ID",
    "100": "creator",
    "100$a": "heading_person",
    "100$c": "heading_person_info",
    "100$d": "heading_person_dates",
    "100$e": "heading_person_role",
    "110$a": "corporate_name",
    "110$e": "corporate_relator_term",
    "110$g": "corporate_info",
    "130$a": "uniform_title",
    "222$a": "key_title",
    "222$b": "key_title_info",
    "240$a": "unifrom_title",
    "240$n": "uniform_title_part_nr",
    "245$a": "title",
    "245$c": "title_responsibility_statement",
    "245$b": "title_remainder",
    "245$h": "title_medium",
    "245$p": "title_part_name",
    "245$n": "title_part_nr",
    "246$a": "title_varform",
    "246$b": "title_varform_remainder",
    "246$f": "title_varform_date_or_nr",
    "246$g": "title_varform_info",
    "246$h": "title_varform_medium",
    "246$i": "title_varform_display_text",
    "246$n": "title_varform_part_nr",
    "250$a": "edition_statement",
    "255$a": "geographic_statement_of_scale",
    "255$b": "geographic_statement_of_projection",
    "255$c": "geographic_statement_of_coordinates",
    "260$a": "publication_place",
    "260$b": "publisher",
    "260$c": "publication_date",
    "260$e": "place_of_manufacture",
    "260$f": "manufacturer",
    "260$g": "date_of_manufacture",
    "264$a": "production_publication_distribution_place",
    "264$b": "producer_publisher_distributer_name",
    "264$c": "production_publication_distribution_date",
    "300$a": "physical_extent",
    "300$b": "physical_details",
    "300$c": "physical_dimensions",
    "300$e": "physical_accompanying_material",
    "310$a": "publication_frequency_current",
    "321$a": "publication_frequency_former",
    "321$b": "publication_frequency_former_dates",
    "336$a": "content_type_term",
    "336$b": "content_type_code",
    "336$2": "content_type_source",
    "337$a": "media_type_term",
    "337$b": "media_type_code",
    "337$2": "media_type_source",
    "338$a": "carrier_type",
    "338$b": "carrier_type_code",
    "338$2": "carrier_type_source",
    "362$a": "dates_of_publication",
    "490$a": "series_statement",
    "490$v": "series_volume",
    "490$x": "series_ISSN",
    "500$a": "general_note",
    "502$a": "dissertation_note",
    "504$a": "bibliography_note",
    "505$a": "formatted_contents_note",
    "505$g": "formatted_contents_note_info",
    "505$r": "formatted_contents_note_statement_of_responsibility",
    "505$t": "formatted_contents_note_title",
    "507$a": "representative_fraction_of_scale_note",
    "508$a": "production_credits_note",
    "510$a": "references_source",
    "510$c": "references_location",
    "511$a": "participant_or_performer_note",
    "514$a": "data_quality_note",
    "515$a": "numbering_peculiarities_note",
    "516$a": "type_of_computer_file_or_data_note",
    "518$a": "date_time_place_of_event_note",
    "520$a": "summary_etc",
    "530$a": "additional_physical_form_available",
    "533$a": "repro_type",
    "533$b": "repro_place",
    "533$c": "repro_agency",
    "533$d": "repro_date",
    "533$n": "repro_note",
    "534$a": "originaL_version_main_entry",
    "534$c": "original_version_distribution",
    "534$n": "original_version_note",
    "534$p": "original_version_introductory_phrase",
    "534$t": "original_version_title_statement",
    "534$z": "original_version_isbn",
    "538$a": "system_details_note",
    "542$l": "copyright_status",
    "542$o": "copyright_research_date",
    "542$q": "copyright_supplying_agency",
    "542$u": "copyright_URI",
    "546$a": "language_note",
    "547$a": "former_title_complexity_note",
    "550$a": "issuing_body_note",
    "580$a": "linking_entry_complexity_note",
    "588$a": "description_note_source",
    "595$a": "typograhy_rara",
    "600": "subject_person",
    "600$a": "subject_person_name",
    "600$c": "subject_person_info",
    "600$t": "subject_person_work_title",
    "600$d": "subject_person_dates",
    "610$a": "subject_corporate_name",
    "611$a": "subject_meeting_name",
    "611$c": "subject_meeting_location",
    "611$d": "subject_meeting_date",
    "611$n": "subject_meeting_nr",
    "648$a": "subject_chronological_term",
    "650$a": "subject_topic",
    "650$0": "subject_topic_thesaurus",
    "651$a": "subject_geographic_name",
    "651$0": "subject_geographic_thesaurus",
    "653$a": "uncontrolled_index_term",
    "655$a": "subject_genre",
    "655$0": "subject_genre_thesaurus",
    "690$a": "undefined_subject1",
    "691$a": "undefined_subject2",
    "692$a": "undefined_subject3",
    "695$a": "undefined_subject4",
    "700": "contributor",
    "700$a": "added_person_name",
    "700$c": "added_person_info",
    "700$d": "added_person_dates",
    "700$e": "added_person_role",
    "700$g": "added_person_info",
    "700$m": "added_person_performance_medium",
    "700$n": "added_person_work_part_nr",
    "700$o": "added_person_arranged_statement",
    "700$p": "added_person_work_part_name",
    "700$r": "added_person_musical_key",
    "700$t": "added_person_work_title",
    "710$a": "added_corporate_name",
    "710$b": "added_corporate_sub_unit",
    "710$e": "added_corporate_relator_term",
    "710$g": "added_corporate_info",
    "711$a": "added_meeting_name",
    "711$c": "added_entry_meeting_location",
    "711$d": "added_meeting_date",
    "740$a": "uncontrolled_related_title",
    "740$h": "uncontrolled_related_title_medium",
    "740$n": "uncontrolled_related_title_part_nr",
    "740$p": "uncontrolled_related_title_part_name",
    "752$c": "added_intermediate_political_jurisdiction",
    "752$d": "added_city",
    "772$t": "supplement_parent_entry_title",
    "772$w": "supplement_parent_entry_record_control_nr",
    "775$t": "other_edition_title",
    "776$a": "additional_physical_form_heading",
    "776$c": "additional_physical_form_info",
    "776$g": "additional_physical_form_related_parts",
    "776$t": "additional_physical_form_title",
    "776$w": "additional_physical_form_record_control_nr",
    "776$x": "additional_physical_form_ISSN",
    "776$z": "additional_physical_form_ISBN",
    "780$g": "preceding_entry_related_parts",
    "780$t": "preceding_entry_title",
    "780$w": "preceding_entry_record_control_nr",
    "785$g": "succeeding_entry_related_parts",
    "785$t": "succeeding_entry_title",
    "785$w": "succeeding_entry_record_control_nr",
    "830$a": "added_series_title",
    "830$v": "added_series_volume",
    "830$x": "added_series_ISSN",
    "856$z": "electronic_access_note",
    "856$u": "electronic_access_URI",
    "866$a": "undefined_rara_field",
    }