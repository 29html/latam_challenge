import time
from datetime import datetime
from functools import partial
from typing import List, Tuple

import pandas as pd
from memory_profiler import memory_usage

from src.common.gcs.google_storage import load_json_from_gcs


def q1_time(
        gcp_file: List[dict],
        dry_mode: bool = True
) -> List[Tuple[datetime.date, str]]:
    """
    Generate a list of the top users by date based on the processed file data.

    This function takes a list of dictionaries containing tweet data and performs the following steps:
    1. If dry_mode is False, print a message indicating that the JSON data of tweets is being processed.
    2. Convert the list of dictionaries into a Pandas DataFrame.
    3. Extract the 'date' field from the DataFrame and convert it to a datetime object.
    4. Extract the 'username' field from the 'user' field in the DataFrame.
    5. Drop the 'user' column from the DataFrame.
    6. Group the DataFrame by 'date' and 'username', and count the occurrences.
    7. Find the top 10 dates with the highest total counts of tweets.
    8. For each of the top dates, find the username with the highest count of tweets.
    9. Return a list of tuples, where each tuple contains a date and the corresponding top username.

    Parameters:
        gcp_file (List[dict]): A list of dictionaries containing tweet data.
        dry_mode (bool, optional): A flag to indicate whether the function is in dry mode. Defaults to True.
    Returns:
        List[Tuple[datetime.date, str]]: A list of tuples containing the date and the top username for that date.
    """
    if not dry_mode:
        print("Processing JSON of tweets")
    try:
        df = pd.DataFrame(gcp_file)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["username"] = df["user"].apply(lambda x: x["username"])
        df.drop(columns=["user"], inplace=True)

        user_date_counter = (
            df.groupby(["date", "username"]).size().reset_index(name="counts")
        )
        top_dates = (
            user_date_counter.groupby("date")["counts"].sum().nlargest(10).index.tolist()
        )

        top_users_by_date = []
        for date in top_dates:
            top_user = user_date_counter.loc[user_date_counter["date"] == date].nlargest(
                1, "counts"
            )
            top_users_by_date.append((date, top_user["username"].iloc[0]))

        return top_users_by_date
    except Exception as e:
        print(f"Error processing the file: {str(e)}")


def main():
    """
    In this main function:
    1. The tweet JSON is obtained from a function that uses the Google Storage service.
    2. A function is used to process the data using Dask and Pandas Dataframe.
    3. While each function is executed, the execution time of each one is calculated.
    4. The memory_profiler library is used to measure the memory usage of file processing.

    This method prints information about the execution with the response of the exercise,
    json download time in seconds, json processing time in seconds and memory used during the process in KB.
    Example:
        Top 10 dates where there are the most tweets:
        [(datetime.date(2021, 2, 12), 'RanbirS00614606'), (datetime.date(2021, 2, 13), 'MaanDee08215437')...]
        Total time downloading data from GCP: 3.91988205909729, sec
        Total time processing tweets: 0.6631908416748047, sec
        Memory used during the process: 1378.859375, KB
    """
    start_load_time = time.time()
    gcp_file = load_json_from_gcs()
    end_load_file = time.time()

    start_processing_time = time.time()
    top_users_by_date = q1_time(
        gcp_file=gcp_file,
        dry_mode=False
    )
    end_processing_time = time.time()

    total_processing_time = end_processing_time - start_processing_time
    total_load_time = end_load_file - start_load_time

    memory_partial = partial(q1_time, gcp_file=gcp_file)
    mem_usage = memory_usage(memory_partial)

    print(f"""
    Top 10 dates where there are the most tweets: 
    {top_users_by_date}
    Total time downloading data from GCP: {total_load_time}, sec
    Total time processing tweets: {total_processing_time}, sec
    Memory used during the process: {mem_usage[0]}, KB
    """)


if __name__ == '__main__':
    main()
