import sys
from pathlib import Path
import json
import re
import numpy as np
import pandas as pd
import isbnlib
from urllib.parse import urlparse
from datetime import datetime

if __name__ == "__main__":
    # when using this script from command line
    import constants
else:
    # when using the clean_dataframe function as imported
    from src.python import constants

current_script_path = Path(__file__)
project_root = current_script_path.parent.parent.parent
read_data_path = project_root / "data" / "converted"
write_data_path = project_root / "data" / "cleaned"
columns_to_keep_file_path = project_root / "config" / "marc_columns_to_keep.json"
column_names_file_path = project_root / "config" / "marc_columns_dict.json"
column_order_file_path = project_root / "config" / "marc_columns_order.json"

MIN_YEAR = 1500
MAX_YEAR = datetime.now().year

def load_converted_data(key: str):
    """Impordib tabeliks teisendatud andmed."""
    #df = pd.read_csv(f"{read_data_path}/{key}_converted.tsv", sep="\t", encoding="utf8", low_memory=False)
    df = pd.read_parquet(f"{read_data_path}/{key}.parquet")

    # # allesjäetavate tulpade nimed tulevad välisest failist
    # with open(columns_to_keep_file_path, "r", encoding="utf8") as f:
    #     columns = json.load(f)["columns"]
    #     columns = [col for col in columns if col in df.columns]
    
    # # jätkame vaid oluliste tulpadega    
    # df = df[columns]
    return df


def roman_to_arabic(roman):
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
    parsed_url = urlparse(url)
    return bool(parsed_url.scheme and parsed_url.netloc)


def clean_008(entry):
    """Eraldab kontrollväljalt vajalikud andmed."""
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
    """Cleans the entry dates from the 008 control field. Use after clean_008()"""
    def parse_entry_dates(date):
        """Convert a date in the format YYMMDD to YYYY-MM-DD"""
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

    # anything greater than the current date must be an insertion error
    today = pd.Timestamp.today().normalize()
    dates.loc[dates > today] = pd.NaT

    return dates


def validate_020(entry):
    """Kontrollib ja puhastab ISBN koode."""
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


def clean_245n(entry: str, pattern=constants.PATTERN_245n):
    """Harmoniseerib pealkirja osanumbri alamvälja."""
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
    """Extracts the original title from the fields 246 (combined from 260$a and 260$g during conversion), 240$a and 130$a"""
    def handle_246(x):
        """Extracts the original title from the 246 field (always followed by the language in square brackets)"""
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


def clean_246(entry):
    """Removes original titles from the 246 field and keeps other varform titles. Use after extract_original_titles()"""
    if isinstance(entry, str):
        return "; ".join([val for val in entry.split("; ") if not re.search(r"\[.+\]$", val)])
    else:
        return pd.NA
    

def clean_250a(entry, pattern=constants.PATTERN_250a):
    """Eraldab editsiooniandmete väljalt kordustrüki arvu."""
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


def add_260abc_264abc(df):
    """Kombineerib tulbad 260abc tulpadega 264abc.
    Seletus: 2022 aastal hakati ilmumisandmeid sisestama 264 väljadele, mistõttu need tuleb varasematega kokku tõsta."""
    for sub in ["a", "b", "c"]:
        df[f"260${sub}"] = df[f"260${sub}"].fillna(df[f"264${sub}"])


def clean_260c(entry, pattern=constants.PATTERN_260c, min_year=MIN_YEAR, max_year=MAX_YEAR):
    """Funktsioon võtab sisse välja 260$c ehk ilmumisaasta kirje ning tagastab aasta ning kümnendi arvulisel kujul."""
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

    # 2022; 2022    
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
            # 2022
            if groups["year"] is not None:
                output_year = int(groups["year"])
            # 196-?
            elif groups["decade"] is not None:
                output_decade = int(groups["decade"].strip("?").replace("-", "0"))

    if output_year:
        if output_year in range(min_year, max_year+1):
            output_decade = output_year // 10 * 10
        else:
            output_year = None

    return (output_year, output_decade)


def clean_300a(entry: str, pattern=constants.PATTERN_300a):
    """Eraldab leheküljenumbrid füüsilise kirjelduse väljalt."""
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


def clean_300b(entry):
    """Kontrollib, kas teosel on illustreerimise märge."""
    if type(entry) == str:
        return True
    else:
        return False
    

def clean_300c(entry):
    """Eraldab sentimeetrid füüsilise ulatuse väljast."""
    if type(entry) == str:
        entry = entry.strip().lstrip()
        # formaat NN cm
        if re.match("\d{1,3}\s?cm", entry):
            entry = entry.split("cm")[0].strip()
        # kataloogimisviga cm -> cn
        elif re.match("\d{1,3}\s?cn", entry):
            entry = entry.split("cn")[0].strip()
        # kahe mõõdu (NN x NN cm) puhul võtame esimese arvu, kuna teine arv märgitakse vaid siis, kui laius on kõrgusest suurem
        elif re.match("\d{1,3}\s?[x]\s?\d{1,3}\s?cm", entry):
            entry = entry.split("x")[0].split(" cm")[0].strip()
        
        if re.match("\d{1,3}$", entry):
            return int(entry)
        else:
            return None


def clean_500a(entry):
    """Eraldab tiraaži hinna ja kirjastiili (fraktuur/antiikva) üldmärkuste väljalt."""
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
        

def clean_504a(entry):
    """Kontrollib, kas kirje sisaldab bibliograafiat ja/või registrit."""
    if type(entry) == str:
        b = ''
        r = ''
        if re.search("[Bb]ibliograafia", entry):
            b = "b"
        if re.search("[Rr]egist(er|rit)", entry):
            r = "r"
        return b+r
    

def clean_533a(entry):
    """Kontrollib, kas kirjest on olemas elektrooniline reproduktsioon."""
    if type(entry) == str:
        return True
    else:
        return False
    

def clean_533d(entry, pattern=constants.PATTERN_533d):
    """Eraldab ja puhastab digiteerimise aasta."""
    if type(entry) != str:
        entry = str(entry)
    match = re.match(pattern, entry)
    if match:
        year = match.string
        if re.match(r"\d{4}(\.0|\w+)", year):
            year = year[:4]
        return year
        

def clean_534c(entry, pattern=constants.PATTERN_534c):
    """Eraldab algupärandi märkusest esmaväljaande aasta, koha ja kirjastuse."""
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


def clean_856u(entry):
    """Puhastab elektroonilise juurdepääsu URLid."""
    if type(entry) == str:
        entry_split = entry.split("; ")
        return "; ".join([url.strip().lstrip() for url in entry_split if is_valid_url(url.strip().lstrip())])
    

def extract_person_info(person_str):
    # Remove any titles enclosed in quotes
    person_str = re.sub(r': ".*?"', '', person_str)

    # Regular expression patterns
    constants.PATTERN_with_date = r'^(.+?) \(([\d?]+)?-([\d?]+)?\)$'
    constants.PATTERN_name_only = r'^(.+?)$'

    # Extract name and birth/death dates
    match = re.match(constants.PATTERN_with_date, person_str)
    if match:
        name, birth_date, death_date = match.groups()
        birth_date = birth_date.strip() if birth_date else None
        death_date = death_date.strip() if death_date else None
        return name.strip(), birth_date, death_date
    
    # Handle the case where only the name exists
    match = re.match(constants.PATTERN_name_only, person_str)
    if match:
        name = match.group(1)
        return name.strip(), None, None

    # Return an error if no pattern matched
    # print(f"Error: '{person_str}' doesn't match expected patterns.")
    return None, None, None


def clean_books(df):

    ### 008: kontrollväli
    if "008" in df.columns:
        print("008: cleaning and harmonizing control field 008")
        df[["date_entered","publication_date_control", "publication_place_control", "language", "is_fiction"]] = df["008"].apply(clean_008).to_list()
        df = df.drop("008", axis=1)
        ### sisestuskuupäev
        df["date_entered"] = clean_entry_dates(df["date_entered"])

    ### 020$a: ISBN
    if "020$a" in df.columns:
        print("020$a: validating ISBN codes")
        df["isbn"] = df["020$a"].apply(validate_020)
        df = df.drop("020$a", axis=1)

    ### 245$n: osa number
    if "245$n" in df.columns:
        print("245$n: cleaning and harmonizing part numeration")
        df["title_part_nr_cleaned"] = df["245$n"].apply(clean_245n)
        # df = df.drop("245$n", axis=1)

    ### 246, 130$a, 240$a: originaali pealkiri ja lisapealkirjad
    if all([col in df.columns for col in ["246", "130$a", "240$a"]]):
        print("Extracting original titles")
        df["title_original"] = extract_original_titles(df)
        df["title_varform"] = df["246"].apply(clean_246)
        df = df.drop(["246", "130$a", "240$a"], axis=1)  

    ### 250$a: editsiooniandmed
    if "250$a" in df.columns:
        print("250$a: cleaning edition statement")
        df["edition_n"] = df["250$a"].apply(clean_250a)

    ### 260, 264: avaldamisinfo
    if all([col in df.columns for col in ["260$a", "260$b", "260$c","264$a", "264$b", "264$c"]]):
        add_260abc_264abc(df)
        df = df.drop(["264$a", "264$b", "264$c"], axis=1)
    if all([col in df.columns for col in ["260$a", "260$b", "260$c"]]):   
        print("260$c: cleaning publishing date")
        df[["publication_date_cleaned", "publication_decade"]] = df["260$c"].apply(clean_260c).to_list()
        
    ### 300$a: lehekülgede arv
    if "300$a" in df.columns:
        print("300$a: extracting page counts")
        df["page_count"] = df["300$a"].apply(clean_300a)
        df = df.drop("300$a", axis=1)

    ### 300$b: illustratsioonide olemasolu
    if "300$b" in df.columns:
        print("300$b: filtering illustrations")
        df["illustrated"] = df["300$b"].apply(clean_300b)
        df = df.drop("300$b", axis=1)

    ### 300$c: füüsilised mõõtmed
    if "300$c" in df.columns:
        print("300$c: extracting physical dimensions")
        df["physical_size"] = df["300$c"].apply(clean_300c)
        df = df.drop("300$c", axis=1)

    ### 500$a: üldmärkused (tiraaž, hind, kirjastiil)
    if "500$a" in df.columns:
        print("500$a: extracting print run, price, typeface")
        df[["print_run", "price", "typeface"]] = df["500$a"].apply(clean_500a).to_list()
        df = df.drop("500$a", axis=1)

    ### 504$a: bibliograafia & register
    if "504$a" in df.columns:
        print("504$a: filtering bibliographies/registers")
        df["bibliography_register"] = df["504$a"].apply(clean_504a)
        df = df.drop("504$a", axis=1)

    ### 533: digitaalne repro
    if "533$a" in df.columns: 
        print("533$a: filtering digital reproductions")
        df["digitized"] = df["533$a"].apply(clean_533a)
        df = df.drop("533$a", axis=1)

    ### 533$d: digiteerimise aasta
    if "533$d" in df.columns:
        print("533$d: extracting digitization year")
        df["digitized_year"] = df["533$d"].apply(clean_533d)
        df = df.drop("533$d", axis=1)

    ### 534$c: algupärandi märkus
    if "534$c" in df.columns:
        print("534$c: extracting original distribution info")
        df[["original_distribution_year", "original_distribution_place", "original_distribution_publisher"]] = df["534$c"].apply(clean_534c).to_list()
        df = df.drop("534$c", axis=1)

    ### 856$u: elektrooniline juurdepääs
    if "856$u" in df.columns:
        df["access_uri"] = df["856$u"].apply(clean_856u)
        df = df.drop("856$u", axis=1)

    ### formaatimine
    df = df.convert_dtypes()

    return df


def clean_persons(df):

    ### 100: retrieving name and dates from 100 subfields
    df[["name", "birth_date", "death_date"]] = df["100"].apply(extract_person_info).to_list()
    df["birth_date"] = df["birth_date"].astype("Int64", errors="ignore")
    df["death_date"] = df["death_date"].astype("Int64", errors="ignore")

    ### 375$a: cleaning and harmonizing gender identities
    df["gender"] = df["375$a"].apply(lambda x: constants.MAPPING_375a.get(x, None))
    df = df.drop("375$a", axis=1)

    return df


def organize_columns(df, collection_type, column_names_file_path=column_names_file_path, column_order_file_path=column_order_file_path):
    ### tulpade ümber nimetamine
    with open(column_names_file_path) as f:
        column_names = json.load(f)
        df = df.rename(columns=column_names)

    ### tulpade järjekord
    with open(column_order_file_path) as f:
        column_order = json.load(f)[collection_type]
        column_order = [col for col in column_order if col in df.columns]
        df = df[column_order]

    return df


if __name__ == "__main__":

    key = sys.argv[1]
    print("Loading data")
    df = load_converted_data(key=key)

    print("Cleaning dataframe")
    if key in ["erb_books", "erb_non_estonian", "erb_all_books"]:
        df = clean_books(df)
        df = organize_columns(df, collection_type="books")
    elif key == "nle_persons":
        df = clean_persons(df)
        df = organize_columns(df, collection_type="nle_persons")
    else:
        print("Warning: there is no separate cleaning function for this collection yet. Cleaning will proceed as if the collection were 'erb_books', but the result may be partially incorrect. Please check the cleaning functions in 'clean.py' for reference.")
        df = clean_books(df)
        df = organize_columns(df, collection_type="books")

    ### salvestamine
    savepath = f"{write_data_path}/{key}.parquet"
    print(f"Saving cleaned file to {savepath}")
    df.to_parquet(savepath)
    
    print("Finished!")