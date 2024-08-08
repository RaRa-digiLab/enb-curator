from src.python.harvest import harvest_oai, collections
from src.python.convert import oai_to_dataframe
from src.python.clean import clean_dataframe, organize_columns
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

        # clean and filter the converted dataframe
        print("\nCleaning dataframe")
        df = clean_dataframe(df)
        df = organize_columns(df)
        df.to_parquet(f"data/cleaned/{key}.parquet")
        
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
        df = clean_dataframe(df)
        df = organize_columns(df)
        df.to_parquet(f"data/cleaned/{key}.parquet")
        # df.to_csv(f"data/cleaned/{key}.tsv", sep="\t", encoding="utf8", index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time  # Calculate the elapsed time
    formatted_time = str(timedelta(seconds=elapsed_time))  # Format the elapsed time
    print(f"\nCompleted in {formatted_time}")