import time
from collections import Counter
from functools import partial
from typing import List, Tuple

from memory_profiler import memory_usage

from src.common.gcs.google_storage import load_json_from_local


def q3_memory(gcp_file: List[dict], dry_mode: bool = True) -> List[Tuple[str, int]]:
    """
    Process the tweet data to generate a list of the top 10 users mentioned.

    This function takes a list of dictionaries containing tweet data and performs the following steps:
    1. If dry_mode is False, print a message indicating that the JSON data of tweets is being processed.
    2. Initialize a Counter object to store user mentions.
    3. Iterate through each tweet in the data and extract the text content.
    4. Extract mentions (words starting with '@') from the text content and update the mention counts.
    5. Find the top 10 users mentioned based on their mention counts using the Counter.
    6. Return a list of tuples containing the top 10 users mentioned and their counts.

    Returns:
        List[Tuple[str, int]]: A list of tuples containing the username and
        its count of mentions (@), representing the top 10 users.
    """
    if not dry_mode:
        print("Processing JSON of tweets")

    try:
        user_mention_counter = Counter()

        for tweet in gcp_file:
            text = tweet.get("content", "")
            mentions = [word[1:] for word in text.split() if word.startswith("@")]
            user_mention_counter.update(mentions)

        top_users = user_mention_counter.most_common(10)

        return top_users
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
            The historical top 10 most influential users (username) based on the count of mentions:
            [('narendramodi', 2261), ('Kisanektamorcha', 1836), ('RakeshTikaitBKU', 1639)..]
            Total time downloading data from GCP: 3.420651912689209, sec
            Total time processing tweets: 0.334871768951416, sec
            Memory used during the process: 1206.078125, KB
    """
    start_load_time = time.time()
    gcp_file = load_json_from_local()
    end_load_file = time.time()

    start_processing_time = time.time()
    top_users = q3_memory(gcp_file=gcp_file, dry_mode=False)
    end_processing_time = time.time()

    total_processing_time = end_processing_time - start_processing_time
    total_load_time = end_load_file - start_load_time

    memory_partial = partial(q3_memory, gcp_file=gcp_file)
    mem_usage = memory_usage(memory_partial)

    print(
        f"""
            The historical top 10 most influential users (username) based on the count of mentions: 
            {top_users}
            Total time downloading data from GCP: {total_load_time}, sec
            Total time processing tweets: {total_processing_time}, sec
            Memory used during the process: {mem_usage[0]}, KB
        """
    )


if __name__ == "__main__":
    main()
