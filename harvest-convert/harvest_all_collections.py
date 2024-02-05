import sys
from harvester import collections, harvest_oai

if __name__ == "__main__":
    if len(sys.argv) > 1:
        start = int(sys.argv[1])
    else:
        start = 0
    for key in list(collections.keys())[start:]:
        print(f"Collecting {collections[key]['title']}")
        harvest_oai(key=key,
                    savepath=f"data/{key}.xml")