"""code to get AEC data for an election code"""
import os
import logging
from dotenv import load_dotenv
from helpers import (
    create_directory_if_not_exists,
    ensure_env_var_exists,
    get_file_from_url,
    merge_csv_files,
)

# settings
INPUT_DIRECTORY = "input"
WORKING_DIRECTORY = "working"
# load env vars
load_dotenv()

# logging
logging.basicConfig()
DEBUG_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")
ELECTION_CODE = ensure_env_var_exists("ELECTION_CODE")
logging.warning("running for election %s", ELECTION_CODE)

print(f"debug level: {DEBUG_LEVEL}, running for election {ELECTION_CODE}")
logging.basicConfig(level=DEBUG_LEVEL.upper())

# make sure g2g
create_directory_if_not_exists(INPUT_DIRECTORY)
create_directory_if_not_exists(WORKING_DIRECTORY)

# get primary results
STATES_TERRITORIES = ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"]
for st in STATES_TERRITORIES:
    URL = f"https://tallyroom.aec.gov.au/Downloads/HouseStateFirstPrefsByPollingPlaceDownload-{ELECTION_CODE}-{st}.csv"
    get_file_from_url(url=URL, output_directory=INPUT_DIRECTORY)


merged_data = merge_csv_files(INPUT_DIRECTORY, "HouseStateFirstPrefs")

# Save merged data to a new CSV file
merged_data.to_csv(
    f"{WORKING_DIRECTORY}{os.sep}{ELECTION_CODE}_primaries.csv", index=False
)
