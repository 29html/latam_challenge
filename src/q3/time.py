import time
from datetime import datetime
from functools import partial
from typing import List, Tuple

import pandas as pd
from memory_profiler import memory_usage

from src.common.gcs.google_storage import load_json_from_gcs


def q3_time(
        gcp_file: List[dict],
        dry_mode: bool = True
) -> List[Tuple[datetime.date, str]]:
    """
    Generate a list of the top 10 users based on the count of mentions (@) in the tweet data.

    Parameters:
        gcp_file (List[dict]): A list of dictionaries containing tweet data.
        dry_mode (bool, optional): A flag to indicate whether the function is in dry mode. Defaults to True.

    Returns:
        List[Tuple[str, int]]: A list of tuples containing the top 10 users and their counts of mentions (@).
    """
    if not dry_mode:
        print("Processing JSON of tweets")

    df = pd.DataFrame(gcp_file)

    df['mentions'] = df['content'].str.findall(r'@(\w+)')

    df_exploded = df.explode('mentions')

    df_filtered = df_exploded[df_exploded['mentions'] != '']

    mention_counts = df_filtered['mentions'].value_counts()

    top_users = mention_counts.head(10)

    top_users_list = [(user, count) for user, count in top_users.items()]

    return top_users_list


def main():
    """
    In this main function:
    1. The tweet JSON is obtained from a function that uses the Google Storage service.
    2. A function is used to process the data using Pandas Dataframe.
    3. While each function is executed, the execution time of each one is calculated.
    4. The memory_profiler library is used to measure the memory usage of file processing.

    This method prints information about the execution with the response of the exercise,
    json download time in seconds, json processing time in seconds and memory used during the process in KB.
    Example:
            The historical top 10 most influential users (username) based on the count of mentions:
            [('narendramodi', 2261), ('Kisanektamorcha', 1836), ('RakeshTikaitBKU', 1639)..]
            Total time downloading data from GCP: 3.838207960128784, sec
            Total time processing tweets: 1.484058141708374, sec
            Memory used during the process 1287.859375, KB
    """
    start_load_time = time.time()
    gcp_file = load_json_from_gcs()
    end_load_file = time.time()

    start_processing_time = time.time()
    top_users = q3_time(
        gcp_file=gcp_file,
        dry_mode=False
    )
    end_processing_time = time.time()

    total_processing_time = end_processing_time - start_processing_time
    total_load_time = end_load_file - start_load_time

    q1_memory_partial = partial(q3_time, gcp_file=gcp_file)
    mem_usage = memory_usage(q1_memory_partial)

    print(
        f"""
            The historical top 10 most influential users (username) based on the count of mentions: 
            {top_users}
            Total time downloading data from GCP: {total_load_time}, sec
            Total time processing tweets: {total_processing_time}, sec
            Memory used during the process {mem_usage[0]}, KB
        """
    )


if __name__ == "__main__":
    main()
