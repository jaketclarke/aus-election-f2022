"""helper functions"""

import datetime
import logging
import os
import time
import shutil
import requests
import pandas as pd
from zoneinfo import ZoneInfo


def wait_with_message(message: str, wait: float) -> None:
    """wait with a console log

    Args:
        message (str): what to say while we wait
        wait (float): how long to wait (in seconds)
    """
    print(f"{message} | waiting {wait} seconds")
    time.sleep(wait)


def get_file_from_url(url: str, output_directory: str) -> None:
    """Takes a url and saves the contents as a file

    Args:
        url (str): url to file
        output_directory (str): where to save the file

    """
    # get url contents
    try:
        r = requests.get(url, allow_redirects=True)
    except ImportError as e:
        raise ImportError(f"Could not get file for url {url}, {e}") from e

    # get name from url
    try:
        filename = url.rsplit("/", 1)[1]
    except ValueError as e:
        raise ValueError(f"Could not get filename from url {url}, {e}") from e

    # save file
    try:
        open(f"{output_directory}{os.sep}{filename}", "wb").write(r.content)
    except RuntimeError as e:
        raise RuntimeError(f"Could not save file {filename} from url {url}, {e}") from e


def get_today_in_melbourne_string() -> str:
    """
    Return today's date in Melbourne - used as a default when getting data
    """
    return datetime.datetime.now(tz=ZoneInfo("Australia/Melbourne")).strftime(
        "%Y-%m-%d"
    )


def get_now_in_melbourne_string() -> str:
    """
    Return today's date in Melbourne - used as a default when getting data
    """
    return datetime.datetime.now(tz=ZoneInfo("Australia/Melbourne")).strftime(
        "%Y-%m-%d-%I-%M-%S"
    )


def create_directory_if_not_exists(directory: str) -> None:
    """Create a directory if it does not exist

    Args:
        directory (str): path to directory
    """

    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"created {directory}")


def destroy_and_remake_directory(directory: str) -> None:
    """Remove a directory if it exists, then create

    Args:
        directory (str): path to directory
    """

    shutil.rmtree(directory, ignore_errors=True)
    print(f"removed {directory}")

    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"created {directory}")


def delete_file_if_exists(filepath: str) -> None:
    """Deletes a file if it exists

    Args:
        filepath (str): file/to/delete.text
    """
    if os.path.exists(filepath):
        os.remove(filepath)


def ensure_environment_variables_exist(environment_vars: list) -> None:
    """Check environment vars are set from a list of options

    Args:
        environment_vars (list): list of environment vars that should exist

    Raises:
        ValueError: Env var not set
    """
    for env_var in environment_vars:
        try:
            if os.getenv(env_var) is None:
                raise ValueError(
                    f"The {env_var} environment variable is required - check .env file or github secrets"
                )
        except ValueError as e:
            raise ValueError(
                f"The {env_var} environment variable is required - check .env file or github secrets, {e}"
            ) from e


def merge_csv_files(directory: str, pattern: str = None) -> pd.DataFrame:
    """Take a directory and merge all the CSV files in it

    Args:
        directory (str): directory to run over

    Returns:
        pd.DataFrame
    """

    if pattern:
        logging.info(f"filtering for pattern '{pattern}'")
        csv_files = [
            file
            for file in os.listdir(directory)
            if file.endswith(".csv") and pattern in file
        ]
    else:
        # Get a list of all CSV files in the directory
        csv_files = [file for file in os.listdir(directory) if file.endswith(".csv")]

    # Initialize an empty list to store DataFrames
    dfs = []

    # Flag to check if it's the first run
    first_run = True

    # Iterate through each CSV file
    for file in csv_files:
        # Read the CSV file
        print(f"running for {file}")
        if first_run:
            # Read the second row to use it as headers
            df = pd.read_csv(os.path.join(directory, file), skiprows=1)
            headers = df.columns
            first_run = False
        else:
            # Read the CSV file skipping the first row
            df = pd.read_csv(os.path.join(directory, file), skiprows=1)
            df.columns = headers  # Set the headers
        # Append the DataFrame to the list
        dfs.append(df)

    # Concatenate all DataFrames in the list
    merged_df = pd.concat(dfs, ignore_index=True)

    return merged_df
