from src.python.harvest import harvest_oai, collections

key = "erb_books"
print(f"Harvesting {collections[key]['title']}")
harvest_oai(key=key, savepath=f"data/raw/{key}.xml")