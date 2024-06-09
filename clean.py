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
# load env vars
load_dotenv()

# logging
logging.basicConfig()
DEBUG_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")
print(f"debug level: {DEBUG_LEVEL}")
logging.basicConfig(level=DEBUG_LEVEL.upper())

# make sure g2g
create_directory_if_not_exists(INPUT_DIRECTORY)

# get primary results
STATES_TERRITORIES = ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"]
for st in STATES_TERRITORIES:
    url = f"https://results.aec.gov.au/27966/Website/Downloads/HouseStateFirstPrefsByPollingPlaceDownload-27966-{st}.csv"
    get_file_from_url(url=url, output_directory=INPUT_DIRECTORY)


merged_data = merge_csv_files(INPUT_DIRECTORY, "HouseStateFirstPrefs")

# Save merged data to a new CSV file
merged_data.to_csv(f"{WORKING_DIRECTORY}{os.sep}primaries.csv", index=False)
