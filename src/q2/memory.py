import time
from collections import Counter
from functools import partial
from typing import List, Tuple

import emoji
from memory_profiler import memory_usage

from src.common.gcs.google_storage import load_json_from_gcs


def q2_memory(gcp_file: List[dict], dry_mode: bool = True) -> List[Tuple[str, int]]:
    """
    Process the tweet data to generate a list of the top emojis used.

    This function takes a list of dictionaries containing tweet data and performs the following steps:
    1. If dry_mode is False, print a message indicating that the JSON data of tweets is being processed.
    2. Initialize a Counter object to count the occurrences of each emoji.
    3. Iterate through each tweet in the data and extract the text content.
    4. Extract emojis from the text content using the emoji library.
    5. Update the emoji counter with the extracted emojis.
    6. Find the top 10 emojis with the highest counts.
    7. Return a list of tuples, where each tuple contains an emoji and its count.

    Parameters:
        gcp_file (List[dict]): A list of dictionaries containing tweet data.
        dry_mode (bool, optional): A flag to indicate whether the function is in dry mode. Defaults to True.

    Returns:
        List[Tuple[str, int]]: A list of tuples containing the emoji and its count, representing the top 10 emojis used.
    """
    if not dry_mode:
        print("Processing JSON of tweets")
    try:
        emoji_counter = Counter()

        for tweet in gcp_file:
            text = tweet.get("content", "")
            emojis = [c for c in text if c in emoji.UNICODE_EMOJI["en"]]
            emoji_counter.update(emojis)

        top_emojis = emoji_counter.most_common(10)

        return top_emojis
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
            Top 10 most used emojis:
            [('🙏', 7286), ('😂', 3072), ('🚜', 2972), ('✊', 2411), ('🌾', 2363)...]
            Total time downloading data from GCP: 21.28494906425476, sec
            Total time processing tweets: 1.9600829124450684, sec
            Memory used during the process: 981.90625, KB
    """
    start_load_time = time.time()
    gcp_file = load_json_from_gcs()
    end_load_file = time.time()

    start_processing_time = time.time()
    top_emojis = q2_memory(gcp_file=gcp_file, dry_mode=False)
    end_processing_time = time.time()

    total_processing_time = end_processing_time - start_processing_time
    total_load_time = end_load_file - start_load_time

    memory_partial = partial(q2_memory, gcp_file=gcp_file)
    mem_usage = memory_usage(memory_partial)

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
