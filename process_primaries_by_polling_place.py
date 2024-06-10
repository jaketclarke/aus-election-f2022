import requests
import os
import re
import pandas as pd
import logging
from dotenv import load_dotenv
from helpers import create_directory_if_not_exists, get_file_from_url, merge_csv_files

# settings
INPUT_DIRECTORY = "input"
WORKING_DIRECTORY = "working"
OUTPUT_DIRECTORY = "output"

create_directory_if_not_exists(OUTPUT_DIRECTORY)

# load env vars
load_dotenv()

# logging
logging.basicConfig()
DEBUG_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")
print(f"debug level: {DEBUG_LEVEL}")
logging.basicConfig(level=DEBUG_LEVEL.upper())

# read data
PRIMARY_DATA_FILEPATH = f"{WORKING_DIRECTORY}{os.sep}primaries.csv"
PRIMARY_DATA_OUTPUT_FILEPATH = (
    f"{OUTPUT_DIRECTORY}{os.sep}f2022_fp_by_polling_place.csv"
)

df_primary = pd.read_csv(PRIMARY_DATA_FILEPATH)

# informal ballots have the position 999
#! todo make this a method - repeated logic
df_formal = df_primary[df_primary["BallotPosition"] != 999]

# add totals
df_formal["TotalFormalVotes"] = df_formal.groupby("PollingPlaceID")[
    "OrdinaryVotes"
].transform("sum")

df_primary["TotalVotes"] = df_primary.groupby("PollingPlaceID")[
    "OrdinaryVotes"
].transform("sum")


df_formal = df_formal[["PollingPlaceID", "CandidateID", "TotalFormalVotes"]]
print(df_formal)

# merge results
df_primary_results = pd.merge(
    left=df_primary, right=df_formal, how="left", on=["PollingPlaceID", "CandidateID"]
)

# add proportions
df_primary_results["TotalVotePc"] = (
    df_primary_results["OrdinaryVotes"] / df_primary_results["TotalVotes"]
)

df_primary_results["FormalVotePc"] = (
    df_primary_results["OrdinaryVotes"] / df_primary_results["TotalFormalVotes"]
)

# output data
df_primary_results.to_csv(PRIMARY_DATA_OUTPUT_FILEPATH, encoding="UTF8", index=False)
