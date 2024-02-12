import sys
import json
import re
import numpy as np
import pandas as pd
import isbnlib

from regex_patterns import PATTERN_245n, PATTERN_260c, PATTERN_300a
MIN_YEAR = 1500
MAX_YEAR = 2024


def load_converted_data(filepath: str):
    """Loads in the data in its raw form after conversion from XML to TSV"""
    df = pd.read_csv(filepath, sep="\t", encoding="utf8")
    with open("../config/marc_columns_to_keep.json", "r", encoding="utf8") as f:
        columns = json.load(f)["columns"]
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


def validate_020(entry):
    """Checks validity of ISBN codes present in the field, returns only valid ones."""
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
    """Combines columns 260abc with columns 264abc in place.
    Explanation: From 2022, publishing info has been catalogued into new MARC fields."""
    for sub in ["a", "b", "c"]:
        df[f"260${sub}"] = df[f"260${sub}"].fillna(df[f"264${sub}"])


def clean_260c(entry, pattern=PATTERN_260c, min_year=MIN_YEAR, max_year=MAX_YEAR):
    """Takes in the raw date and returns publishing year and decade as integers."""
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
    """Extracts page numbers from physical extent field."""
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
    if type(entry) == str:
        return True
    else:
        return False
    

def clean_300c(entry):
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



if __name__ == "__main__":

    filepath = sys.argv[1]
    df = load_converted_data(filepath=filepath)

    ### 020$a: ISBN
    print("020$a: validating ISBN codes")
    df["020$a"] = df["020$a"].apply(validate_020)
    df.rename(columns={"020$a": "ISBN"})

    ### keeled
    ### kood siia...

    ### 245$n: part number
    print("245$a: cleaning and harmonizing part numeration")
    df["title_part_nr"] = df["245$n"].apply(clean_245n)

    ### 260, 264: publishing info
    add_260abc_264abc(df)
    print("260$c: cleaning publishing date")
    df.rename(columns={"260$c": "publishing_year"})
    df[["publishing_year_cleaned", "publishing_decade"]] = df["publishing_year"].apply(clean_260c).to_list()

    ### 300$a: page count
    print("300$a: extracting page counts")
    df["page_count"] = df["300$a"].apply(clean_300a)

    ### 300$b: illustrated Y/N
    print("300$b: filtering illustrations")
    df["illustrated"] = df["300$b"].apply(clean_300b)

    ### 300$c: physical extent
    df["physical_size"] = df["300$c"].apply(clean_300c)



