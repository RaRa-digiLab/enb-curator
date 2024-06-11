from src.python.harvest import harvest_oai, collections
from src.python.convert import oai_to_dataframe

import sys

key = sys.argv[1]

print(f"Harvesting {collections[key]['title']}")
harvest_oai(key=key, savepath=f"data/raw/{key}.xml")

print(f"Converting {key} to dataframe")
df = oai_to_dataframe(f"data/raw/{key}.xml", marc_threshold=0.05, replace_columns=False)
df.to_parquet(f"data/converted/{key}.parquet")

print("Finished")