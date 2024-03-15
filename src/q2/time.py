import time
from datetime import datetime
from functools import partial
from typing import List, Tuple

import emoji
import pandas as pd
from memory_profiler import memory_usage

from src.common.gcs.google_storage import load_json_from_gcs


def q2_time(
        gcp_file: List[dict],
        dry_mode: bool = True
) -> List[Tuple[datetime.date, str]]:
    """
    Generate a list of the top 10 emojis used in the tweet data.

    Parameters:
        gcp_file (List[dict]): A list of dictionaries containing tweet data.
        dry_mode (bool, optional): A flag to indicate whether the function is in dry mode. Defaults to True.

    Returns:
        List[Tuple[str, int]]: A list of tuples containing the top 10 emojis and their counts.
    """
    if not dry_mode:
        print("Processing JSON of tweets")

    emoji_counts = {}

    for tweet in gcp_file:
        text = tweet.get("content", "")
        emojis = [c for c in text if c in emoji.UNICODE_EMOJI["en"]]
        for emoji_char in emojis:
            if emoji_char in emoji_counts:
                emoji_counts[emoji_char] += 1
            else:
                emoji_counts[emoji_char] = 1

    df = pd.DataFrame(emoji_counts.items(), columns=["emoji", "count"])

    top_emojis = df.nlargest(10, "count")

    top_emojis_list = top_emojis[["emoji", "count"]].apply(tuple, axis=1).tolist()

    return top_emojis_list


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
            Top 10 most used emojis:
            [('🙏', 7286), ('😂', 3072), ('🚜', 2972), ('✊', 2411), ('🌾', 2363)...]
            Total time downloading data from GCP: 22.1781587600708, sec
            Total time processing tweets: 1.0470860004425049, sec
            Memory used during the process 1017.375, KB
    """
    start_load_time = time.time()
    gcp_file = load_json_from_gcs()
    end_load_file = time.time()

    start_processing_time = time.time()
    top_emojis = q2_time(
        gcp_file=gcp_file,
        dry_mode=False
    )
    end_processing_time = time.time()

    total_processing_time = end_processing_time - start_processing_time
    total_load_time = end_load_file - start_load_time

    q1_memory_partial = partial(q2_time, gcp_file=gcp_file)
    mem_usage = memory_usage(q1_memory_partial)

    print(
        f"""
            Top 10 most used emojis: 
            {top_emojis}
            Total time downloading data from GCP: {total_load_time}, sec
            Total time processing tweets: {total_processing_time}, sec
            Memory used during the process {mem_usage[0]}, KB
        """
    )


if __name__ == "__main__":
    main()
