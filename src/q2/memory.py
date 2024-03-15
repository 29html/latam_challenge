import time
from collections import Counter
from functools import partial
from typing import List, Tuple

import emoji
from memory_profiler import memory_usage

from src.common.gcs.google_storage import load_json_from_gcs


def q2_memory(gcp_file: List[dict], dry_mode: bool = True) -> List[Tuple[str, int]]:
    """
    Generate a list of the top emojis used based on the processed file data.

    Parameters:
        gcp_file (List[dict]): A list of dictionaries containing tweet data.
        dry_mode (bool, optional): A flag to indicate whether the function is in dry mode. Defaults to True.

    Returns:
        List[Tuple[str, int]]: A list of tuples containing the emoji and its count, representing the top 10 emojis used.
    """
    if not dry_mode:
        print("Processing JSON of tweets")

    emoji_counter = Counter()

    for tweet in gcp_file:
        text = tweet.get("content", "")
        emojis = [c for c in text if c in emoji.UNICODE_EMOJI["en"]]
        emoji_counter.update(emojis)

    top_emojis = emoji_counter.most_common(10)

    return top_emojis


def main():
    """
    In this main function:
    1. The tweet JSON is obtained from a function that uses the Google Storage service.
    2. A function is used to process the data using Dask and Pandas Dataframe.
    3. While each function is executed, the execution time of each one is calculated.
    4. The memory_profiler library is used to measure the memory usage of file processing.
    """
    start_load_time = time.time()
    gcp_file = load_json_from_gcs()
    end_load_file = time.time()

    start_processing_time = time.time()
    top_emojis = q2_memory(gcp_file=gcp_file, dry_mode=False)
    end_processing_time = time.time()

    total_processing_time = end_processing_time - start_processing_time
    total_load_time = end_load_file - start_load_time

    q1_memory_partial = partial(q2_memory, gcp_file=gcp_file)
    mem_usage = memory_usage(q1_memory_partial)

    print(
        f"""
            Top 10 most used emojis: 
            {top_emojis}
            Total time downloading data from GCP: {total_load_time}, sec
            Total time processing tweets: {total_processing_time}, sec
            Memory used during the process: {mem_usage[0]}, KB
        """
    )


if __name__ == "__main__":
    main()
