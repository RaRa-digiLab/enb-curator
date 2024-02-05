import glob
from converter import oai_to_dataframe

for file in glob.glob("data/*.xml"):
    fname = file.split("\\")[-1]
    print(f"Converting {fname}")
    df = oai_to_dataframe(file)
    df.to_csv(f'data/converted/{fname.replace(".xml", ".tsv")}',
                sep="\t", encoding="utf8", index=False)
    
print("Finished")