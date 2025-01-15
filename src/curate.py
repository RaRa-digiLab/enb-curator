import sys
from pathlib import Path
import json
import re
import numpy as np
import pandas as pd
import csv
import isbnlib
from urllib.parse import urlparse
from datetime import datetime
from tqdm import tqdm
import requests
from requests.exceptions import RequestException

if __name__ == "__main__":
    # when using this script from command line
    import constants
else:
    # when using the clean_dataframe function as imported
    from src import constants

current_script_path = Path(__file__)
project_root = current_script_path.parent.parent
read_data_path = project_root / "data" / "converted"
write_data_path = project_root / "data" / "curated"

columns_to_keep_file_path = project_root / "config" / "marc_columns_to_keep.json"
column_names_file_path = project_root / "config" / "marc_columns_dict.json"
column_order_file_path = project_root / "config" / "marc_columns_order.json"

placenames_file_path = project_root / "config" / "placenames" / "placenames_harmonized.tsv"
coordinates_file_path = project_root / "config" / "placenames" / "placenames_coordinates.tsv"
persons_links_file_path = project_root / "config" / "persons" / "persons_id_links.tsv"
persons_gender_file_path = project_root / "config" / "persons" / "persons_gender.tsv"
publisher_rules_file_path = project_root / "config" / "publishers" / "publisher_harmonize_rules.tsv"
publisher_harmonization_file_path = project_root / "config" / "publishers" / "publisher_harmonization_mapping.json"
publisher_similarity_groups_file_path = project_root / "config" / "publishers" / "publisher_similarity_groups.tsv"

MIN_YEAR = 1500
MAX_YEAR = datetime.now().year

def load_converted_data(key: str):
    """Imports the converted data into a DataFrame."""
    df = pd.read_parquet(f"{read_data_path}/{key}.parquet")
    return df

def roman_to_arabic(roman):
    """Converts a Roman numeral to an Arabic numeral."""
    roman_numerals = {
        'I': 1,
        'V': 5,
        'X': 10,
        'L': 50,
        'C': 100,
        'D': 500,
        'M': 1000
    }
    prev_value = 0
    total = 0
    for char in reversed(roman):
        value = roman_numerals[char] 
        if value < prev_value:
            total -= value
        else:
            total += value 
        prev_value = value
    return total

def is_valid_url(url):
    """Checks if a URL is valid."""
    parsed_url = urlparse(url)
    return bool(parsed_url.scheme and parsed_url.netloc)

def extract_control_field_008_data(entry):
    """Extracts necessary data from the control field 008.

    MARC field(s): 008
    """
    if type(entry) == str:
        if len(entry) in range(38, 41):
            date_entered = entry[0:6]
            publication_date = entry[7:11]
            publication_place = entry[15:18]
            publication_language = entry[35:38]
            literary_form = entry[33]

            # check if fiction
            is_fiction = None
            if literary_form in [0, "0", "e", "i", "s"]:
                is_fiction = False
            elif literary_form in [1, "1", "d", "f", "h", "j", "p"]:
                is_fiction = True

            return date_entered, publication_date, publication_place, publication_language, is_fiction
    return None, None, None, None, None

def clean_entry_dates(dates):
    """Cleans the entry dates from the 008 control field. Use after extract_control_field_008_data()."""
    def parse_entry_dates(date):
        """Convert a date in the format YYMMDD to YYYY-MM-DD."""
        if isinstance(date, str):
            if re.match("\d{6}", date):
                year = int(date[:2])
                if year >= 70:  # Assuming no overlap and 1970 is the threshold
                    return '19' + date[:2] + "-" + date[2:4] + "-" + date[4:6]
                else:
                    return '20' + date[:2] + "-" + date[2:4] + "-" + date[4:6]
        return None

    dates = dates.apply(parse_entry_dates)
    dates = pd.to_datetime(dates, errors='coerce')

    # Anything greater than the current date must be an insertion error
    today = pd.Timestamp.today().normalize()
    dates.loc[dates > today] = pd.NaT

    return dates

def validate_isbn(entry):
    """Validates and cleans ISBN codes.

    MARC field(s): 020$a
    """
    try:
        if type(entry) == str:
            entry_split = np.array(entry.split("; "))
            valid_isbns = []
            for code in entry_split:
                code = isbnlib.clean(code)
                if isbnlib.is_isbn10(code):
                    valid_isbns.append(code)
                elif isbnlib.is_isbn13(code):
                    valid_isbns.append(code)
            return "; ".join(valid_isbns)
        else:
            return None
    except:
        print(f"Could not process", entry)

def clean_title_part_number(entry: str, pattern=constants.PATTERN_245n):
    """Harmonizes the part number subfield of the title.

    MARC field(s): 245$n
    """
    if type(entry) == str:
        match = re.search(pattern, entry)
        if match:
            groups = match.groupdict()
            n = None
            p = None

            if groups["n"]:
                if groups["araabia"]:
                    n = groups["araabia"]
                elif groups["rooma"]:
                    n = str(roman_to_arabic(groups["rooma"]))
                elif groups["arvsna"]:
                    for key, val in groups.items():
                        if re.search("a\d{1,2}", key):
                            if val is not None:
                                n = key.lstrip("a")
                                break
                elif groups["AB"]:
                    if groups["AB"] == "A":
                        n = "1"
                    elif groups["AB"] == "B":
                        n = "2"
                    
            if groups["p"]:
                p = f" [{groups['p'].lstrip('[').strip(']')}]"
            else:
                p = ''

            return n+p

def extract_original_titles(df):
    """Extracts the original title from the fields 246 (combined from 260$a and 260$g during conversion), 240$a, and 130$a."""
    def handle_246(x):
        """Extracts the original title from the 246 field (always followed by the language in square brackets)."""
        if isinstance(x, str):
            processed_values = [val.rsplit(" [", maxsplit=1)[0] for val in x.split("; ") if re.search(r"\[.+\]$", val)]
            result = "; ".join(processed_values)
            return result if result else pd.NA
        else:
            return pd.NA

    # Add values from 246 to a new column
    df["title_original"] = pd.NA
    df["title_original"] = df["246"].apply(handle_246)

    # Where 246 did not provide an original title, use 240 and then 130
    df["title_original"] = df["title_original"].fillna(df["240$a"]).fillna(df["130$a"])

    return df["title_original"]

def clean_varform_titles(entry):
    """Removes original titles from the 246 field and keeps other variant titles.

    MARC field(s): 246
    """
    if isinstance(entry, str):
        return "; ".join([val for val in entry.split("; ") if not re.search(r"\[.+\]$", val)])
    else:
        return pd.NA

def extract_edition_number(entry, pattern=constants.PATTERN_250a):
    """Extracts the number of reprints from the edition statement field.

    MARC field(s): 250$a
    """
    if type(entry) == str:
        match = re.search(pattern, entry)
        if match:
            groups = match.groupdict()
            n = None
            tr = None

            if groups["n"]:
                if groups["araabia"]:
                    n = groups["araabia"]
                    n = "".join([char for char in n if char.isnumeric()])
                elif groups["arvsna"]:
                    for key, val in groups.items():
                        if re.search("a\d{1,2}", key):
                            if val is not None:
                                n = key.lstrip("a")
                                break
            if groups["tr"]:
                tr = groups["tr"]
                if n is None:
                    n = "+"
            if n or tr:
                return f"{n or ''} [{tr or ''}]"
                
            return "+"
        return None

def combine_publishing_fields(df):
    """Combines columns 260$a, 260$b, 260$c with columns 264$a, 264$b, 264$c.

    Explanation: In 2022, publishing data started to be entered into field 264, so these need to be combined with earlier data.

    MARC field(s): 260$a, 260$b, 260$c, 264$a, 264$b, 264$c
    """
    for sub in ["a", "b", "c"]:
        df[f"260${sub}"] = df[f"260${sub}"].fillna(df[f"264${sub}"])

def extract_publication_year(entry, pattern=constants.PATTERN_260c, min_year=MIN_YEAR, max_year=MAX_YEAR):
    """Extracts the publication year and decade in numerical form from the field 260$c.

    MARC field(s): 260$c
    """
    output_year = None
    output_decade = None

    if type(entry) != str:
        if isinstance(entry, list):
            entry = "; ".join(entry)
        else:
            try:
                entry = str(entry)
            except:
                return None, None

    # Handle multiple years separated by ";"
    if ";" in entry:
        entry_split = entry.split("; ")
        years = []
        for part in entry_split:
            match = re.search(pattern, part)
            if match:
                part_year = match.groupdict()["year"]
                if part_year is not None:
                    if part_year not in years:
                        if match.groupdict()["copyright"] is None:
                            years.append(part_year)
                        else:
                            if len(years) == 0:
                                years.append(part_year)

        if len(years) == 1:
            output_year = int(years[0])
        else:
            try:
                output_year = min([int(y) for y in years])
            except ValueError:
                return None, None

    else:
        match = re.search(pattern, entry)
        if match:
            groups = match.groupdict()
            # Single year
            if groups["year"] is not None:
                output_year = int(groups["year"])
            # Decade
            elif groups["decade"] is not None:
                output_decade = int(groups["decade"].strip("?").replace("-", "0"))

    if output_year:
        if output_year in range(min_year, max_year+1):
            output_decade = output_year // 10 * 10
        else:
            output_year = None

    return (output_year, output_decade)

def extract_page_count(entry: str, pattern=constants.PATTERN_300a):
    """Extracts page numbers from the physical description field.

    MARC field(s): 300$a
    """
    if type(entry) == str:
        match = re.search(pattern, entry)
        if match:
            lk = None
            groups = match.groupdict()
            if groups["uhik"]:
                if groups["uhik"] in ["l", "lk", "lehte", "lehekülg", "lehekülge", "nummerdamata lehekülge"]:
                    if groups["vahemik"]:
                        start, end = groups["vahemik"].split("-")
                        start = "".join([char for char in start if char.isnumeric()])
                        lk = int(end) - int(start)
                    elif groups["arv"]:
                        lk = int(groups["arv"])
                    elif groups["sulud"]:
                        lk = int(groups["sulud"].lstrip("[").strip("]"))
            elif groups["vahemik"]:
                start, end = groups["vahemik"].split("-")
                start = "".join([char for char in start if char.isnumeric()])
                lk = int(end) - int(start)

            return lk

def has_illustrations(entry):
    """Checks whether the work has an illustration note.

    MARC field(s): 300$b
    """
    if type(entry) == str:
        return True
    else:
        return False

def extract_physical_dimensions(entry):
    """Extracts centimeters from the physical extent field.

    MARC field(s): 300$c
    """
    if type(entry) == str:
        entry = entry.strip().lstrip()
        # Format NN cm
        if re.match("\d{1,3}\s?cm", entry):
            entry = entry.split("cm")[0].strip()
        # Cataloging error cm -> cn
        elif re.match("\d{1,3}\s?cn", entry):
            entry = entry.split("cn")[0].strip()
        # For dimensions (NN x NN cm), take the first number
        elif re.match("\d{1,3}\s?[x]\s?\d{1,3}\s?cm", entry):
            entry = entry.split("x")[0].split(" cm")[0].strip()
        
        if re.match("\d{1,3}$", entry):
            return int(entry)
        else:
            return None

def extract_print_run_price_typeface(entry):
    """Extracts print run, price, and typeface (fraktur/antiqua) from the general notes field.

    MARC field(s): 500$a
    """
    tiraaz = None
    hind = None
    kirjastiil = None
    if type(entry) == str:

        constants.PATTERN_500a_tiraaz = re.compile(r"(?P<tiraaz>\d+(\.\d+)?)\s(eks\.?)")
        match = re.search(constants.PATTERN_500a_tiraaz, entry)
        if match:
            tiraaz = int(match.groupdict()["tiraaz"].replace('.', ''))

        constants.PATTERN_500a_hind = re.compile(r"(?P<rubla>\d\s(rbl\.?|rubla))?\s*(?P<kop>\d{1,2}\skop)")
        match = re.search(constants.PATTERN_500a_hind, entry)
        if match:
            hind = match.string[match.span()[0]:match.span()[1]]

        constants.PATTERN_500a_kirjastiil = re.compile(r"(?P<kirjastiil>[Ff]raktuur|[Aa]ntiikva)")
        match = re.search(constants.PATTERN_500a_kirjastiil, entry)
        if match:
            kirjastiil = (match.groupdict()["kirjastiil"].lower()[0])

    return (tiraaz, hind, kirjastiil)        

def extract_bibliography_index_info(entry):
    """Checks whether the record contains bibliography and/or index.

    MARC field(s): 504$a
    """
    if type(entry) == str:
        b = ''
        r = ''
        if re.search("[Bb]ibliograafia", entry):
            b = "b"
        if re.search("[Rr]egist(er|rit)", entry):
            r = "r"
        return b+r

def has_electronic_reproduction(entry):
    """Checks whether an electronic reproduction exists from the record.

    MARC field(s): 533$a
    """
    if type(entry) == str:
        return True
    else:
        return False

def extract_digitization_year(entry, pattern=constants.PATTERN_533d):
    """Extracts and cleans the digitization year.

    MARC field(s): 533$d
    """
    if type(entry) != str:
        entry = str(entry)
    match = re.match(pattern, entry)
    if match:
        year = match.string
        if re.match(r"\d{4}(\.0|\w+)", year):
            year = year[:4]
        return year

def extract_original_publication_info(entry, pattern=constants.PATTERN_534c):
    """Extracts the year, place, and publisher of the first edition from the original work note.

    MARC field(s): 534$c
    """
    year = None
    place = None
    publisher = None
    if type(entry) == str:
        entry = entry.lstrip().replace('[', '').replace(']', '')
        match = re.search(pattern, entry)
        if match:
            matchgroups = match.groupdict()
            if matchgroups["year"]:
                year = int(matchgroups["year"])
            if matchgroups["place"]:
                place = matchgroups["place"]
            if matchgroups["publisher"]:
                publisher = matchgroups["publisher"]
            if matchgroups["range"]:
                year = int(matchgroups["range"].split("-")[0])

    return (year, place, publisher)

def clean_electronic_access_urls(entry):
    """Cleans electronic access URLs.

    MARC field(s): 856$u
    """
    if type(entry) == str:
        entry_split = entry.split("; ")
        return "; ".join([url.strip().lstrip() for url in entry_split if is_valid_url(url.strip().lstrip())])

def resolve_multiple_person_ids(entry):
    if isinstance(entry, str):
        entry_split = entry.split("; ")
        if len(entry_split) > 1:
            valid_ids = [id for id in entry_split if id[0] == "a"]
            return valid_ids[0]

    return entry        

def extract_person_info(person_str, role=True):
    # Remove any titles enclosed in quotes
    person_str = re.sub(r': ".*?"', '', person_str)

    # Helper function to process individual dates
    def process_date(date_str):
        if not date_str:
            return None, False  # Return None and BC indicator as False
        date_str = date_str.strip()
        is_bc = False

        # Remove 'u. ' prefix indicating uncertainty
        date_str = re.sub(r'^u\. ?', '', date_str)

        # Check for 'e. Kr' (BC dates) and 'p. Kr' (AD dates)
        bc_match = re.search(r'e\. ?Kr\.?', date_str, re.IGNORECASE)
        ad_match = re.search(r'p\. ?Kr\.?', date_str, re.IGNORECASE)
        if bc_match:
            is_bc = True
        elif ad_match:
            is_bc = False  # Explicitly marked as AD

        # Remove 'e. Kr' or 'p. Kr' suffixes
        date_str = re.sub(r'(e\. ?Kr\.?|p\. ?Kr\.?)', '', date_str, flags=re.IGNORECASE).strip()

        # Convert date string to integer
        try:
            date_int = int(date_str)
            return date_int, is_bc
        except ValueError:
            return None, False  # Return None if the date is not a valid integer

    # Regular expression pattern to match the entire string
    # Matches: Name (birth_date - death_date) [role]
    pattern = r'^(.+?)\s*\((.*?)\)\s*(?:\[(.+?)\])?$'

    match = re.match(pattern, person_str)
    if match:
        name, date_range, role_str = match.groups()
        name = name.strip()
        role_str = role_str.strip().lower() if role_str and role else None

        # Split the date range into birth and death dates
        birth_date_str, _, death_date_str = date_range.partition('-')

        # Process birth and death dates
        birth_date, birth_is_bc = process_date(birth_date_str)
        death_date, death_is_bc = process_date(death_date_str)

        # If the death date is BC and the birth date is not explicitly AD, assume birth date is BC
        if death_is_bc and not birth_is_bc and birth_date is not None:
            birth_is_bc = True
            birth_date = -birth_date
        elif birth_is_bc and birth_date is not None:
            birth_date = -birth_date

        # If the death date is BC, make it negative
        if death_is_bc and death_date is not None:
            death_date = -death_date

        if role:
            return (name, birth_date, death_date, role_str)
        else:
            return (name, birth_date, death_date)

    # Handle cases with only the name and optional role
    # Matches: Name [role] or Name
    pattern_name_role = r'^(.+?)\s*(?:\[(.+?)\])?$'
    match = re.match(pattern_name_role, person_str)
    if match:
        name, role_str = match.groups()
        name = name.strip()
        role_str = role_str.strip().lower() if role_str and role else None

        if role:
            return (name, None, None, role_str)
        else:
            return (name, None, None)

    # Return None values if no pattern matched
    if role:
        return (None, None, None, None)
    else:
        return (None, None, None)
        
def check_if_posthumous(creators, publication_date, contributors=None):
    if not isinstance(publication_date, int):
        return None

    if isinstance(creators, str):
        creators_list = creators.split("; ")
    else:
        creators_list = []

    # Option to add contributors whose role is defined as "autor"
    if isinstance(contributors, str):
        if "[autor]" in contributors:
            for c in contributors.split("; "):
                if extract_person_info(c, role=True)[3] == "autor":
                    creators_list.append(c)

    if len(creators_list) == 0:
        return None

    # Extract death dates
    death_dates = [extract_person_info(c, role=True)[2] for c in creators_list]

    # In the case that all death dates are present
    if all(isinstance(y, int) for y in death_dates):
        last_death = max(death_dates)
        return last_death < publication_date

    # Assume that authors marked only by century are published posthumously
    if any(["saj." in c for c in creators_list]):
        return True

    # Assume that no author lives longer than 120 years
    birth_dates = [extract_person_info(c, role=True)[1] for c in creators_list]
    if all(isinstance(x, int) for x in birth_dates):
        if all(publication_date - y > 120 for y in birth_dates):
            return True

    # Otherwise, a missing death date is justified and means that one of the authors is/was alive 
    return False

def harmonize_placenames(place_column):
    """Uses an external authority file to map placenames to their harmonized versions, accounting for multiple names in a single cell."""
    # Load mapping data from file
    with open(placenames_file_path, "r", encoding="utf8") as f:
        place_names = pd.read_csv(f, sep="\t", encoding="utf8")

    # Create a dictionary to map original to harmonized names
    place_names = place_names.query("place_harmonized.notna()")
    mapping = dict(zip(place_names["place_original"], place_names["place_harmonized"]))

    # Split, map, and rejoin using vectorized operations, handling NA values
    harmonized_placenames = (
        place_column
        .apply(lambda x: "; ".join(mapping.get(place, place) for place in x.split("; ")) if pd.notna(x) else x)
    )

    # Remove duplicate placenames within each cell
    harmonized_placenames = (
        harmonized_placenames
        .apply(lambda x: "; ".join(dict.fromkeys(x.split("; "))) if pd.notna(x) else x)
    )
    
    return harmonized_placenames

def get_coordinates(place_column):
    """Uses the external authority file to map placenames to their coordinates, handling multiple placenames in a single cell."""
    # Load mapping data from file
    with open(coordinates_file_path, "r", encoding="utf8") as f:
        coordinates = pd.read_csv(f, sep="\t", encoding="utf8")
    
    # Create dictionaries to map placename to lat and lon
    mapping_lat = dict(zip(coordinates["place_harmonized"], coordinates["lat"]))
    mapping_lon = dict(zip(coordinates["place_harmonized"], coordinates["lon"]))

    # Define a function to retrieve the first available coordinates from multiple placenames
    def get_first_coordinates(places):
        for place in places.split("; "):
            lat = mapping_lat.get(place)
            lon = mapping_lon.get(place)
            if lat is not None and lon is not None:
                return lat, lon
        return None, None  # Return None if no valid coordinates are found

    # Apply the function, handling NA values
    coordinates = place_column.apply(lambda x: get_first_coordinates(x) if pd.notna(x) else (None, None))
    
    # Convert the resulting list of tuples into a DataFrame with columns 'lat' and 'lon'
    coordinates_df = pd.DataFrame(coordinates.tolist(), columns=["lat", "lon"])
    
    return coordinates_df

def harmonize_publishers_with_rules(publishers_column):
    # Read the rules file
    rules = pd.read_csv(
        publisher_rules_file_path,
        sep="\t",
        encoding="utf8",
        quoting=csv.QUOTE_NONE,
        escapechar="\\"
    )

    # Correct double backslashes in patterns
    rules['find_this'] = rules['find_this'].str.replace(r"\\\\", r"\\", regex=True)
    rules['replace_with'] = rules['replace_with'].str.replace(r"\\\\", r"\\", regex=True)
    rules = rules.fillna("").astype(str)

    # Split the publishers_column into individual publishers
    all_publishers = publishers_column.dropna().str.split(';').explode().str.strip().unique()

    # Create a Series with the unique individual publishers
    publishers_original = pd.Series(all_publishers)
    publishers = publishers_original.str.lower()
    publishers = publishers.str.replace('"', '')
    publishers = publishers.fillna("").astype(str)

    ### Apply 'exact' rules first
    exact_rules = rules.query("type == 'exact'")
    for ix, row in exact_rules.iterrows():
        publishers = publishers.replace(row["find_this"], row["replace_with"])
    # Trim whitespace and remove extra spaces
    publishers = publishers.str.strip()
    publishers = publishers.str.replace("  ", " ", regex=True)

    ### Apply 'regex_replace' rules next
    regex_replace_rules = rules.query("type == 'regex_replace'")
    for ix, row in regex_replace_rules.iterrows():
        try:
            find_pattern = row["find_this"]
            publishers = publishers.apply(
                lambda x: row["replace_with"] if re.search(find_pattern, x) else x
            )
        except Exception as e:
            print(f"Error with pattern '{row['find_this']}' replacing with '{row['replace_with']}': {e}")
    # Trim whitespace and remove extra spaces
    publishers = publishers.str.strip()
    publishers = publishers.str.replace("  ", " ", regex=True)

    ### Apply 'regex_partial' rules last
    while True:
        previous_publishers = publishers.copy()
        regex_partial_rules = rules.query("type == 'regex_partial'")
        for ix, row in regex_partial_rules.iterrows():
            try:
                find_pattern = row["find_this"]
                publishers = publishers.str.replace(find_pattern, row["replace_with"], regex=True)
            except Exception as e:
                print(f"Error with pattern '{row['find_this']}' replacing with '{row['replace_with']}': {e}")
        # Trim whitespace and remove extra spaces
        publishers = publishers.str.strip()
        publishers = publishers.str.replace("  ", " ", regex=True)

        differences = publishers != previous_publishers
        if differences.any():  # If there are any differences
            continue
        else:
            # Stop if no changes were made
            break

    # Map the harmonized publishers back to the original individual publishers
    rule_based_mapping = dict(zip(publishers_original, publishers))

    # Function to harmonize a cell containing multiple publishers
    def harmonize_cell(cell):
        if pd.isnull(cell):
            return cell
        publishers_in_cell = [p.strip() for p in cell.split('; ')]
        harmonized_publishers_in_cell = [rule_based_mapping.get(p, p) for p in publishers_in_cell]
        return '; '.join(harmonized_publishers_in_cell)

    # Apply the harmonization to the original publishers_column
    harmonized_publishers = publishers_column.apply(harmonize_cell)

    return harmonized_publishers

def harmonize_publishers_simple(publishers_column):
    with open(publisher_harmonization_file_path, "r", encoding="utf8") as f:
        mapping = json.load(f)
    return publishers_column.map(mapping)

def group_publishers_by_similarity(df):
    groups_df = pd.read_csv(
        publisher_similarity_groups_file_path, 
        sep="\t", 
        encoding="utf8", 
        dtype=str
    )

    counts = groups_df['similarity_group'].value_counts()
    valid_similarity_groups = counts[counts >= 2].index
    filtered_groups_df = groups_df[groups_df['similarity_group'].isin(valid_similarity_groups)]

    location_publisher_to_group = {}

    for location, group in filtered_groups_df.groupby('harm_name'):
        publisher_to_group = dict(zip(group['standardizing_name'], group['similarity_group']))
        location_publisher_to_group[location] = publisher_to_group

    publisher_similarity_groups = []

    for idx, row in df.iterrows():
        location = row['publication_place_harmonized']
        publishers = row['publisher_harmonized']

        if pd.isnull(publishers):
            publisher_similarity_groups.append(None)
            continue

        publisher_list = [p.strip() for p in publishers.split(';')]
        publisher_to_group = location_publisher_to_group.get(location, {})
        publisher_group_list = []

        for publisher in publisher_list:
            similarity_group = publisher_to_group.get(publisher, publisher)  # Use publisher if no mapping found
            publisher_group_list.append(similarity_group)

        similarity_group_str = '; '.join(publisher_group_list)
        publisher_similarity_groups.append(similarity_group_str)

    df['publisher_similarity_group'] = publisher_similarity_groups

    return df

def get_viaf_and_wkp_ids(id_number):
    try:
        jsonld_url = f'https://viaf.org/viaf/sourceID/ERRR|{id_number}/viaf.jsonld'
        response = requests.get(jsonld_url)

        if response.status_code == 200:
            jsonld_data = response.json()
            viaf_id = next((item.get('identifier', 'NA') for item in jsonld_data.get('@graph', []) if item.get('@type') == "schema:Person"), 'NA')
            wkp_id = next((url.split('/')[-1] for item in jsonld_data.get('@graph', []) for url in item.get('sameAs', []) if 'wikidata.org' in url), 'NA')
            return viaf_id, wkp_id

    except RequestException:
        return None, None
    
    return None, None

def update_authority_and_df(input_df, strip_prefix=True):
    """
    Updates the external authority file with new ids found in input_df and then uses it to update the dataframe.
    """
    # Step 1: Load external authority file and identify new ids
    try:
        links = pd.read_csv(persons_links_file_path, sep="\t", encoding="utf8")
        existing_ids = set(links["rara_id"])
    except Exception as e:
        print(f"VIAF and Wikidata linking: Error loading authority file: {e}")
        return input_df

    # Step 2: Filter input_df to only include ids not in the authority file
    missing_entries_df = input_df[~input_df['id'].isin(existing_ids)].copy()[['id']]

    if missing_entries_df.empty:
        print("VIAF and Wikidata linking: Person IDs authority file is up to date (no new persons found since last ingest).")
        return input_df

    # Initialize list to hold successful new entries
    new_entries = []
    print(f"VIAF and Wikidata linking: Found {len(missing_entries_df)} new persons. Attempting to link.")

    progress_bar = tqdm(
    missing_entries_df.iterrows(),
    total=len(missing_entries_df),
    desc="Linking new persons (press Ctrl+C to skip)"
    )

    # Iterate through missing entries and update IDs using get_viaf_and_wkp_ids
    for index, row in progress_bar:
        if strip_prefix:
            id_number = row['id'].lstrip("a")
        else:
            id_number = row['id']
        try:
            #print(id_number)
            viaf_id, wkp_id = get_viaf_and_wkp_ids(id_number)
            if viaf_id is not None and wkp_id is not None and (viaf_id != 'NA' or wkp_id != 'NA'):
                new_entries.append({'rara_id': row['id'], 'viaf_id': viaf_id, 'wkp_id': wkp_id})
            else:
                new_entries.append({'rara_id': row['id'], 'viaf_id': 'NA', 'wkp_id': 'NA'})
        except KeyboardInterrupt:
            print("VIAF and Wikidata linking: Linking interrupted by user.")
            progress_bar.close()
            break
        except Exception as e:
            tqdm.write(f"VIAF and Wikidata linking: Error linking ID {id_number}: {e}")

    # Step 3: Append the new rows to the authority file and save it (only keep 'rara_id', 'viaf_id', 'wkp_id' columns)
    updated_links = links  # Default to existing links in case no successful entries are found
    try:
        if new_entries:
            new_entries_df = pd.DataFrame(new_entries)
            updated_links = pd.concat([links, new_entries_df], ignore_index=True).fillna("NA")[['rara_id', 'viaf_id', 'wkp_id']]
            updated_links.to_csv(persons_links_file_path, sep="\t", index=False, encoding="utf8")
            print(f"VIAF and Wikidata linking: Successfully linked {len(new_entries)} new persons. Authority file updated.")
        else:
            print("VIAF and Wikidata linking: Linking failed for new persons. Some persons in the dataset will not have VIAF and/or Wikidata links.")
    except Exception as e:
        print(f"VIAF and Wikidata linking: Error updating authority file: {e}")

    # Step 4: Use the updated authority file to update the original dataframe like in get_persons_links
    try:
        viaf_mapping = dict(zip(updated_links["rara_id"], updated_links["viaf_id"]))
        wkp_mapping = dict(zip(updated_links["rara_id"], updated_links["wkp_id"]))

        # Map IDs to VIAF and Wikidata links, defaulting to None if not found
        input_df['viaf_id'] = input_df['id'].map(viaf_mapping)
        input_df['wkp_id'] = input_df['id'].map(wkp_mapping)
    except Exception as e:
        print(f"VIAF and Wikidata linking: Error updating dataframe with authority links: {e}")
    
    return input_df

def apply_gender_mapping(id_column):
    """Reads external gender data (combined from NLE, VIAF, Wikidata)and applies the mapping to the dataframe."""
    gender_data = pd.read_csv(persons_gender_file_path, sep="\t", encoding="utf8")
    gender_mapping = dict(zip(gender_data["rara_id"], gender_data["gender"]))
    return df["id"].map(gender_mapping)


def curate_books(df):

    ### 008: control field
    if "008" in df.columns:
        print("Cleaning and harmonizing control field 008")
        df[["date_entered","publication_date_control", "publication_place_control", "language", "is_fiction"]] = df["008"].apply(extract_control_field_008_data).to_list()
        df = df.drop("008", axis=1)
        # Entry date
        df["date_entered"] = clean_entry_dates(df["date_entered"])

    ### 020$a: ISBN
    if "020$a" in df.columns:
        print("Validating ISBN codes")
        df["isbn"] = df["020$a"].apply(validate_isbn)
        df = df.drop("020$a", axis=1)

    ### 245$n: part number
    if "245$n" in df.columns:
        print("Cleaning and harmonizing part numeration")
        df["title_part_nr_cleaned"] = df["245$n"].apply(clean_title_part_number)
        # df = df.drop("245$n", axis=1)

    ### 246, 130$a, 240$a: original title and variant titles
    if all([col in df.columns for col in ["246", "130$a", "240$a"]]):
        print("Extracting original titles")
        df["title_original"] = extract_original_titles(df)
        df["title_varform"] = df["246"].apply(clean_varform_titles)
        df = df.drop(["246", "130$a", "240$a"], axis=1)  

    ### 250$a: edition statement
    if "250$a" in df.columns:
        print("Cleaning edition statement")
        df["edition_n"] = df["250$a"].apply(extract_edition_number)

    ### 260, 264: publication info
    if all([col in df.columns for col in ["260$a", "260$b", "260$c","264$a", "264$b", "264$c"]]):
        combine_publishing_fields(df)
        df = df.drop(["264$a", "264$b", "264$c"], axis=1)
    if all([col in df.columns for col in ["260$a", "260$b", "260$c"]]):   
        print("Cleaning publishing date")
        df[["publication_date_cleaned", "publication_decade"]] = df["260$c"].apply(extract_publication_year).to_list()
        # Convert to Int64 right away for check_if_posthumous to work later
        df[["publication_date_cleaned", "publication_decade"]] = df[["publication_date_cleaned", "publication_decade"]].astype("Int64", errors="ignore")
        
    ### 300$a: page count
    if "300$a" in df.columns:
        print("Extracting page counts")
        df["page_count"] = df["300$a"].apply(extract_page_count)
        df = df.drop("300$a", axis=1)

    ### 300$b: illustrations
    if "300$b" in df.columns:
        print("Filtering illustrations")
        df["is_illustrated"] = df["300$b"].apply(has_illustrations)
        df = df.drop("300$b", axis=1)

    ### 300$c: physical dimensions
    if "300$c" in df.columns:
        print("Extracting physical dimensions")
        df["physical_size"] = df["300$c"].apply(extract_physical_dimensions)
        df = df.drop("300$c", axis=1)

    ### 500$a: general notes (print run, price, typeface)
    if "500$a" in df.columns:
        print("Extracting print run, price, typeface")
        df[["print_run", "price", "typeface"]] = df["500$a"].apply(extract_print_run_price_typeface).to_list()
        df = df.drop("500$a", axis=1)

    ### 504$a: bibliography & index
    if "504$a" in df.columns:
        print("Filtering bibliographies/registers")
        df["has_bibliography_register"] = df["504$a"].apply(extract_bibliography_index_info)
        df = df.drop("504$a", axis=1)

    ### 533$a: digital reproduction
    if "533$a" in df.columns: 
        print("Filtering digital reproductions")
        df["is_digitized"] = df["533$a"].apply(has_electronic_reproduction)
        df = df.drop("533$a", axis=1)

    ### 533$d: digitization year
    if "533$d" in df.columns:
        print("Extracting digitization year")
        df["digitized_year"] = df["533$d"].apply(extract_digitization_year)
        df = df.drop("533$d", axis=1)

    ### 534$c: original publication info
    if "534$c" in df.columns:
        print("Extracting original distribution info")
        df[["original_distribution_year", "original_distribution_place", "original_distribution_publisher"]] = df["534$c"].apply(extract_original_publication_info).to_list()
        df["original_distribution_place"] = harmonize_placenames(df["original_distribution_place"])
        df = df.drop("534$c", axis=1)

    ### 856$u: electronic access
    if "856$u" in df.columns:
        print("Cleaning digital access URIs")
        df["access_uri"] = df["856$u"].apply(clean_electronic_access_urls)
        df = df.drop("856$u", axis=1)

    ### Define posthumously published records
    if all([col in df.columns for col in ["100", "publication_date_cleaned"]]):
        print("Defining posthumously published records")
        df["is_posthumous"] = df.apply(lambda x: check_if_posthumous(x["100"], x["publication_date_cleaned"]), axis=1)

    ### Harmonize publication places
    if "260$a" in df.columns:
        print("Harmonizing and linking publication places")
        df["publication_place_harmonized"] = harmonize_placenames(df["260$a"])
        df[["publication_place_latitude", "publication_place_longitude"]] = get_coordinates(df["publication_place_harmonized"])

    if "260$e" in df.columns:
        print("Harmonizing manufacturing places")
        df["manufacturing_place"] = harmonize_placenames(df["260$e"])
        df = df.drop("260$e", axis=1)

    if "260$b" in df.columns:
        print("Harmonizing publishers")
        df["publisher_harmonized"] = harmonize_publishers_simple(df["260$b"])
        df = group_publishers_by_similarity(df)

    ### Formatting
    df = df.convert_dtypes()

    return df

def curate_persons(df):

    ### resolve incorrect ids
    print("Resolving entries with multiple IDs")
    df["id"] = df["001"].apply(resolve_multiple_person_ids)
    df = df.drop("001", axis=1)

    ### 100: retrieving name and dates from 100 subfields
    print("Extracting names and dates")
    df[["name", "birth_date", "death_date"]] = df["100"].apply(extract_person_info, args=(False,)).to_list()
    df["birth_date"] = df["birth_date"].astype("Int64", errors="ignore")
    df["death_date"] = df["death_date"].astype("Int64", errors="ignore")

    ### 375$a: cleaning and harmonizing gender identities
    print("Cleaning and harmonizing gender identities")
    df["gender"] = apply_gender_mapping(df["id"])
    df = df.drop("375$a", axis=1)

    ### Add VIAF and Wikidata links from authority file
    print("Adding VIAF and Wikidata links")
    df = update_authority_and_df(df, strip_prefix=False)

    return df

def organize_columns(df, collection_type, column_names_file_path=column_names_file_path, column_order_file_path=column_order_file_path):
    """Renames columns and orders them according to the specified configuration."""
    with open(column_names_file_path) as f:
        column_names = json.load(f)
        df = df.rename(columns=column_names)

    with open(column_order_file_path) as f:
        column_order = json.load(f)[collection_type]
        column_order = [col for col in column_order if col in df.columns]
        df = df[column_order]

    return df

if __name__ == "__main__":

    key = sys.argv[1]
    print("Loading data")
    df = load_converted_data(key=key)

    if key in ["enb_books", "enb_non_estonian", "enb_all_books"]:
        df = curate_books(df)
        df = organize_columns(df, collection_type="books")
    elif key == "persons":
        df = curate_persons(df)
        df = organize_columns(df, collection_type="persons")
    else:
        print("Warning: some of the columns in this collection do not yet have custom cleaning functions. Cleaning will proceed as if the collection were 'enb_books', but the result may be partially incorrect. Please check 'curate.py' for reference.")
        df = curate_books(df)
        df = organize_columns(df, collection_type="books")

    ### Saving
    savepath = f"{write_data_path}/{key}.parquet"
    print(f"Saving cleaned file to {savepath}")
    df.to_parquet(savepath)
    
    print("Finished!")
