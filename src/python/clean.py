import sys
from pathlib import Path
import json
import re
import numpy as np
import pandas as pd
import isbnlib
from urllib.parse import urlparse

from regex_patterns import PATTERN_245n, PATTERN_260c, PATTERN_300a, PATTERN_533d, PATTERN_534c

MIN_YEAR = 1500
MAX_YEAR = 2024

current_script_path = Path(__file__)
project_root = current_script_path.parent.parent.parent
read_data_path = project_root / "data" / "interim"
write_data_path = project_root / "data" / "interim"
columns_to_keep_file_path = project_root / "config" / "marc_columns_to_keep.json"
column_names_file_path = project_root / "config" / "marc_columns_dict.json"


def load_converted_data(key: str):
    """Impordib toorandmed pärast XML-ist TSV-sse konverteerimist."""
    df = pd.read_csv(f"{read_data_path}/{key}.tsv", sep="\t", encoding="utf8", low_memory=False)

    # allesjäetavate tulpade nimed tulevad välisest failist
    with open(columns_to_keep_file_path, "r", encoding="utf8") as f:
        columns = json.load(f)["columns"]
    
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
            groups = match.groupdict()
            lk = None
            if groups["arv"]:
                if groups["uhik"]:
                    if groups["uhik"] in ["l", "lk", "lehekülge", "nummerdamata lehekülge"]:
                        lk = int(groups["arv"].lstrip("[").strip("]"))
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
            place = matchgroups["place"]
            publisher = matchgroups["publisher"]
            if matchgroups["range"]:
                year = int(matchgroups["range"].split("-")[0])

    return (year, place, publisher)


def clean_856u(entry):
    """Puhastab elektroonilise juurdepääsu URLid."""
    if type(entry) == str:
        entry_split = entry.split("; ")
        return "; ".join([url.strip().lstrip() for url in entry_split if is_valid_url(url.strip().lstrip())])


if __name__ == "__main__":

    key = sys.argv[1]
    print("Loading data")
    df = load_converted_data(key=key)

    ### 020$a: ISBN
    print("020$a: validating ISBN codes")
    df["isbn"] = df["020$a"].apply(validate_020)
    df = df.drop("020$a", axis=1)

    ### keeled
    ### kood siia...

    ### 245$n: osa number
    print("245$a: cleaning and harmonizing part numeration")
    df["title_part_nr"] = df["245$n"].apply(clean_245n)
    df = df.drop("245$n", axis=1)

    ### 260, 264: avaldamisinfo
    add_260abc_264abc(df)
    print("260$c: cleaning publishing date")
    df[["publishing_year_cleaned", "publishing_decade"]] = df["260$c"].apply(clean_260c).to_list()
    df = df.drop(["260$a", "260$b", "260$c", "264$a", "264$b", "264$c"], axis=1)

    ### 300$a:lehekülgede arv
    print("300$a: extracting page counts")
    df["page_count"] = df["300$a"].apply(clean_300a)
    df = df.drop("300$a", axis=1)

    ### 300$b: lllustratsioonide olemasolu
    print("300$b: filtering illustrations")
    df["illustrated"] = df["300$b"].apply(clean_300b)
    df = df.drop("300$b", axis=1)

    ### 300$c: füüsilised mõõtmed
    print("300$c: extracting physical dimensions")
    df["physical_size"] = df["300$c"].apply(clean_300c)
    df = df.drop("300$c", axis=1)

    ### 500$a: üldmärkused (tiraaž, hind, kirjastiil)
    print("500$a: extracting print run, price, typeface")
    df[["print_run", "price", "typeface"]] = df["500$a"].apply(clean_500a).to_list()
    df = df.drop("500$a", axis=1)

    ### 504$a: bibliograafia & register
    print("504$a: filtering bibliographies/registers")
    df["bibliography_register"] = df["504$a"].apply(clean_504a)
    df = df.drop("504$a", axis=1)

    ### 533: digitaalne repro
    print("533$a: filtering digital reproductions")
    df["digitized"] = df["533$a"].apply(clean_533a)
    df["digitized_year"] = df["533$d"].apply(clean_533d)
    df = df.drop(["533$a", "533$d"], axis=1)

    ### 534$c: algupärandi märkus
    print("534$c: extracting original distribution info")
    df[["original_distribution_year", "original_distribution_place", "original_distribution_publisher"]] = df["534$c"].apply(clean_534c).to_list()
    df = df.drop("534$c", axis=1)

    ### 534$l: autoriõiguse märge
    ### kood siia...

    ### märksõnad
    ### kood siia

    ### 856$u: elektrooniline juurdepääs
    df["access_uri"] = df["856$u"].apply(clean_856u)
    df = df.drop("856$u", axis=1)

    ### tulpade ümber nimetamine
    ### kood siia, kui kõik tulbad on üle käidud

    ### formaatimine
    df = df.convert_dtypes()

    ### salvestamine
    savepath = f"{write_data_path}/{key}.parquet"
    print(f"Saving cleaned file to {savepath}")
    df.to_parquet(f"{write_data_path}/{key}_cleaned.parquet")
    
    print("Finished!")




