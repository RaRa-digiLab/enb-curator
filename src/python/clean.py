import sys
from pathlib import Path
import json
import re
import numpy as np
import pandas as pd
import isbnlib
from urllib.parse import urlparse

if __name__ == "__main__":
    from regex_patterns import PATTERN_245n, PATTERN_250a, PATTERN_260c, PATTERN_300a, PATTERN_533d, PATTERN_534c
else:
    from src.python.regex_patterns import PATTERN_245n, PATTERN_250a, PATTERN_260c, PATTERN_300a, PATTERN_533d, PATTERN_534c

MIN_YEAR = 1500
MAX_YEAR = 2024

current_script_path = Path(__file__)
project_root = current_script_path.parent.parent.parent
read_data_path = project_root / "data" / "converted"
write_data_path = project_root / "data" / "cleaned"
columns_to_keep_file_path = project_root / "config" / "marc_columns_to_keep.json"
column_names_file_path = project_root / "config" / "marc_columns_dict.json"
column_order_file_path = project_root / "config" / "marc_columns_order.json"


def load_converted_data(key: str):
    """Impordib tabeliks teisendatud andmed."""
    #df = pd.read_csv(f"{read_data_path}/{key}_converted.tsv", sep="\t", encoding="utf8", low_memory=False)
    df = pd.read_parquet(f"{read_data_path}/{key}.parquet")

    # allesjäetavate tulpade nimed tulevad välisest failist
    with open(columns_to_keep_file_path, "r", encoding="utf8") as f:
        columns = json.load(f)["columns"]
        columns = [col for col in columns if col in df.columns]
    
    # jätkame vaid oluliste tulpadega    
    df = df[columns]
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
            publication_date = entry[7:11]
            publication_place = entry[15:18]
            publication_language = entry[35:38]
            literary_form = entry[33] # only for books - change for other material!

            # check if fiction (only for books)
            is_fiction = None
            if literary_form in [0, "0", "e", "i", "s"]:
                is_fiction = False
            elif literary_form in [1, "1", "d", "f", "h", "j", "p"]:
                is_fiction = True

            return publication_date, publication_place, publication_language, is_fiction
    return None, None, None, None


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


def clean_245n(entry: str, pattern=PATTERN_245n):
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
        

def clean_250a(entry, pattern=PATTERN_250a):
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


def clean_260c(entry, pattern=PATTERN_260c, min_year=MIN_YEAR, max_year=MAX_YEAR):
    """Teeb toorest ilmumisaasta kirjest puhastatud aastaarvu ja kümnendi (int)."""
    output_year = None
    output_decade = None

    if type(entry) != str:
        entry = str(entry)

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
            output_year = min([int(y) for y in years])

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


def clean_300a(entry: str, pattern=PATTERN_300a):
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

        pattern_500a_tiraaz = re.compile(r"(?P<tiraaz>\d+(\.\d+)?)\s(eks\.?)")
        match = re.search(pattern_500a_tiraaz, entry)
        if match:
            tiraaz = int(match.groupdict()["tiraaz"].replace('.', ''))

        pattern_500a_hind = re.compile(r"(?P<rubla>\d\s(rbl\.?|rubla))?\s*(?P<kop>\d{1,2}\skop)")
        match = re.search(pattern_500a_hind, entry)
        if match:
            hind = match.string[match.span()[0]:match.span()[1]]

        pattern_500a_kirjastiil = re.compile(r"(?P<kirjastiil>[Ff]raktuur|[Aa]ntiikva)")
        match = re.search(pattern_500a_kirjastiil, entry)
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
    

def clean_533d(entry, pattern=PATTERN_533d, min_year=2000, max_year=MAX_YEAR):
    """Eraldab ja puhastab digiteerimise aasta."""
    if type(entry) != str:
        entry = str(entry)
    match = re.match(pattern, entry)
    if match:
        year = match.string
        if re.match(r"\d{4}(\.0|\w+)", year):
            year = year[:4]
        if year in range(min_year, max_year):
            return year
        

def clean_534c(entry, pattern=PATTERN_534c):
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


def clean_dataframe(df):

    ### 008: kontrollväli
    if "008" in df.columns:
        print("008: cleaning and harmonizing control field 008")
        df[["publication_date_control", "publication_place_control", "language_control", "is_fiction"]] = df["008"].apply(clean_008).to_list()
        df = df.drop("008", axis=1)

    ### 020$a: ISBN
    if "020$a" in df.columns:
        print("020$a: validating ISBN codes")
        df["isbn"] = df["020$a"].apply(validate_020)
        df = df.drop("020$a", axis=1)

    ### keeled
    ### kood siia...

    ### 245$n: osa number
    if "245$n" in df.columns:
        print("245$n: cleaning and harmonizing part numeration")
        df["title_part_nr_cleaned"] = df["245$n"].apply(clean_245n)
        # df = df.drop("245$n", axis=1)

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
    if "533$a" in df.columns and "533$d" in df.columns: 
        print("533$a: filtering digital reproductions")
        df["digitized"] = df["533$a"].apply(clean_533a)
        df["digitized_year"] = df["533$d"].apply(clean_533d)
        df = df.drop(["533$a", "533$d"], axis=1)

    ### 534$c: algupärandi märkus
    if "534$c" in df.columns:
        print("534$c: extracting original distribution info")
        df[["original_distribution_year", "original_distribution_place", "original_distribution_publisher"]] = df["534$c"].apply(clean_534c).to_list()
        df = df.drop("534$c", axis=1)

    ### 534$l: autoriõiguse märge
    ### kood siia...

    ### märksõnad
    ### kood siia

    ### 856$u: elektrooniline juurdepääs
    if "856$u" in df.columns:
        df["access_uri"] = df["856$u"].apply(clean_856u)
        df = df.drop("856$u", axis=1)

    ### formaatimine
    df = df.convert_dtypes()

    return df


def organize_columns(df, column_names_file_path=column_names_file_path, column_order_file_path=column_order_file_path):
    ### tulpade ümber nimetamine
    with open(column_names_file_path) as f:
        column_names = json.load(f)
        df = df.rename(columns=column_names)

    ### tulpade järjekord
    with open(column_order_file_path) as f:
        column_order = json.load(f)["columns"]
        column_order = [col for col in column_order if col in df.columns]
        df = df[column_order]

    return df


if __name__ == "__main__":

    key = sys.argv[1]
    print("Loading data")
    df = load_converted_data(key=key)

    print("Cleaning dataframe")
    df = clean_dataframe(df)

    print("Organizing columns")
    df = organize_columns(df)

    ### salvestamine
    savepath = f"{write_data_path}/{key}.parquet"
    print(f"Saving cleaned file to {savepath}")
    df.to_parquet(savepath)
    
    print("Finished!")