from src.harvest import harvest_oai, collections
from src.convert import oai_to_dataframe
import src.curate as curate
from datetime import timedelta
import time
import sys

if __name__ == "__main__":
    start_time = time.time()
    key = sys.argv[1]

    if key not in collections.keys():
        raise ValueError(f"Invalid collection: {key}. Valid collections are: {['enb_books'] + list(collections.keys())}")

    if key == "enb_books":
        for k in ["enb_estonian_books", "enb_non_estonian_books"]:
            # harvest and save the raw XML file
            print(f"\nHarvesting {collections[k]['title']}")
            harvest_oai(key=k, savepath=f"data/raw/{k}.xml")

            # take the raw XML file, convert it to a dataframe and save it
            print(f"\nConverting {k} to dataframe")
            df = oai_to_dataframe(f"data/raw/{k}.xml", rename_columns=False)
            df.to_parquet(f"data/converted/{k}.parquet")
        
        # concatenate the dataframes for cleaning
        import pandas as pd
        enb_est = pd.read_parquet("data/converted/enb_estonian_books.parquet")
        enb_non = pd.read_parquet("data/converted/enb_non_estonian_books.parquet")
        df = pd.concat([enb_est, enb_non]).reset_index(drop=True)
        del(enb_est, enb_non) # free up RAM
        df.to_parquet(f"data/converted/{key}.parquet")

        # clean and filter the converted dataframe
        print("\nCleaning dataframe")
        df = curate.curate_books(df)
        df = curate.organize_columns(df, collection_type="books")
        df.to_parquet(f"data/curated/{key}.parquet")
        

    elif key == "persons":
        # harvest and save the raw XML file
        print(f"\nHarvesting {collections[key]['title']}")
        harvest_oai(key=key, savepath=f"data/raw/{key}.xml")

        # take the raw XML file, convert it to a dataframe and save it
        print(f"\nConverting {key} to dataframe")
        df = oai_to_dataframe(f"data/raw/{key}.xml", rename_columns=False)
        df.to_parquet(f"data/converted/{key}.parquet")

        # clean and filter the converted dataframe
        print("\nCleaning dataframe")
        df = curate.curate_persons(df)
        df = curate.organize_columns(df, collection_type="persons")
        df.to_parquet(f"data/curated/{key}.parquet")

    else:
        # harvest and save the raw XML file
        print(f"\nHarvesting {collections[key]['title']}")
        harvest_oai(key=key, savepath=f"data/raw/{key}.xml")

        # take the raw XML file, convert it to a dataframe and save it
        print(f"\nConverting {key} to dataframe")
        df = oai_to_dataframe(f"data/raw/{key}.xml", rename_columns=False)
        df.to_parquet(f"data/converted/{key}.parquet")

        # clean and filter the converted dataframe
        print("\nProcessing dataframe")
        print("Warning: some of the columns in this collection do not yet have custom cleaning functions. Cleaning will proceed as if the collection were 'enb_books', but the result may be partially incorrect. Please check 'curate.py' for reference.")
        df = curate.curate_books(df)
        df = curate.organize_columns(df, collection_type="books")
        df.to_parquet(f"data/curated/{key}.parquet")
        # df.to_csv(f"data/curated/{key}.tsv", sep="\t", encoding="utf8", index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time  # Calculate the elapsed time
    formatted_time = str(timedelta(seconds=elapsed_time))  # Format the elapsed time
    print(f"\nCompleted in {formatted_time}")