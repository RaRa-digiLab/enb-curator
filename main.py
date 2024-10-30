from src.harvest import harvest_oai, collections
from src.convert import oai_to_dataframe
import src.curate as curate
from datetime import timedelta
import time
import sys

if __name__ == "__main__":
    start_time = time.time()
    key = sys.argv[1]

    if key == "erb_all_books":
        for k in ["erb_books", "erb_non_estonian"]:
            # harvest and save the raw XML file
            print(f"\nHarvesting {collections[k]['title']}")
            harvest_oai(key=k, savepath=f"data/raw/{k}.xml")

            # take the raw XML file, convert it to a dataframe and save it
            print(f"\nConverting {k} to dataframe")
            df = oai_to_dataframe(f"data/raw/{k}.xml", rename_columns=False)
            df.to_parquet(f"data/converted/{k}.parquet")
        
        # concatenate the dataframes for cleaning
        import pandas as pd
        erb_est = pd.read_parquet("data/converted/erb_books.parquet")
        erb_non = pd.read_parquet("data/converted/erb_non_estonian.parquet")
        df = pd.concat([erb_est, erb_non]).reset_index(drop=True)
        del(erb_est, erb_non) # free up RAM
        df.to_parquet(f"data/converted/{key}.parquet")

        # clean and filter the converted dataframe
        print("\nCleaning dataframe")
        df = curate.curate_books(df)
        df = curate.organize_columns(df, collection_type="books")
        df.to_parquet(f"data/curated/{key}.parquet")
        

    elif key == "nle_persons":
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
        df = curate.organize_columns(df, collection_type="nle_persons")
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
        print("\nCleaning dataframe")
        print("Warning: there is no separate cleaning function for this collection yet. Cleaning will proceed as if the collection were 'erb_books', but the result may be partially incorrect. Please check the cleaning functions in 'clean.py' for reference.")
        df = curate.curate_books(df)
        df = curate.organize_columns(df, collection_type="books")
        df.to_parquet(f"data/curated/{key}.parquet")
        # df.to_csv(f"data/curated/{key}.tsv", sep="\t", encoding="utf8", index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time  # Calculate the elapsed time
    formatted_time = str(timedelta(seconds=elapsed_time))  # Format the elapsed time
    print(f"\nCompleted in {formatted_time}")