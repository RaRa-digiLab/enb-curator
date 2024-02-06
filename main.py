from src.python.harvest import harvest_oai, collections
from src.python.convert import oai_to_dataframe

key = "erb_books"

print(f"Harvesting {collections[key]['title']}")
harvest_oai(key=key, savepath=f"data/raw/{key}.xml")

print(f"Converting {key} to dataframe")
df = oai_to_dataframe(f"data/raw/{key}.xml", marc_threshold=0.1, replace_columns=False)
df.to_csv(f"data/interim/{key}.tsv", sep="\t", encoding="utf8", index=False)

print("Finished")