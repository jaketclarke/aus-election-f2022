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
PRIMARY_DATA_FILEPATH = f"{OUTPUT_DIRECTORY}{os.sep}f2022_fp_by_polling_place.csv"
PRIMARY_VOTETYPE_DATA_FILEPATH = f"{OUTPUT_DIRECTORY}{os.sep}f2022_fp_by_vote_type.csv"
SA1_DATA = f"{OUTPUT_DIRECTORY}{os.sep}2022-federal-election-votes-sa1.csv"
df = pd.read_csv(PRIMARY_VOTETYPE_DATA_FILEPATH, skiprows=1)

# make matching df
primary_data = pd.read_csv(PRIMARY_DATA_FILEPATH)
other_data = pd.read_csv(PRIMARY_VOTETYPE_DATA_FILEPATH)

primary_data = primary_data[
    ["DivisionNm", "PollingPlaceID", "PartyAb", "TotalVotePc", "FormalVotePc"]
]

print(primary_data)
# sa1 rename pp_id PollingPlaceID
# pp_nm PollingPlaceNm
# ccd_id SA1_2021
# votes SA1_Total_Votes
