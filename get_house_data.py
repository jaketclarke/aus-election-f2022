"""code to get AEC data for an election code"""

import os
import logging
from dotenv import load_dotenv
from helpers import (
    destroy_and_remake_directory,
    ensure_env_var_exists,
    get_file_from_url,
    merge_csv_files,
)

# settings
INPUT_DIRECTORY = "input"
WORKING_DIRECTORY = "working"

# load env vars
load_dotenv()

# setup logging
LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")
logging.basicConfig(
    level=LOGGING_LEVEL.upper(),
    handlers=[logging.StreamHandler()],
)
logging.info("debug level: %s", LOGGING_LEVEL)

ELECTION_CODE = ensure_env_var_exists("ELECTION_CODE")
logging.warning("running for election %s", ELECTION_CODE)

# if the election is ongoing, we use a different url
ELECTION_LIVE = os.getenv("ELECTION_LIVE")

if ELECTION_LIVE:
    BASE_URL = "https://tallyroom.aec.gov.au/"
else:
    BASE_URL = f"https://results.aec.gov.au/{ELECTION_CODE}/Website/"


# make sure g2g
destroy_and_remake_directory(INPUT_DIRECTORY)
destroy_and_remake_directory(WORKING_DIRECTORY)

# get primary results
STATES_TERRITORIES = ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"]
for st in STATES_TERRITORIES:
    URL = f"{BASE_URL}Downloads/HouseStateFirstPrefsByPollingPlaceDownload-{ELECTION_CODE}-{st}.csv"
    get_file_from_url(url=URL, output_directory=INPUT_DIRECTORY)


merged_data = merge_csv_files(INPUT_DIRECTORY, "HouseStateFirstPrefs")

# Save merged data to a new CSV file
merged_data.to_csv(
    f"{WORKING_DIRECTORY}{os.sep}HouseStateFirstPrefsByPollingPlaceDownload-{ELECTION_CODE}-merged.csv", index=False
)

# get tcp by pp
URL = f"{BASE_URL}Downloads/HouseTcpByCandidateByPollingPlaceDownload-{ELECTION_CODE}.csv"
get_file_from_url(url=URL, output_directory=INPUT_DIRECTORY)

# get tcp by vote type
URL = f"{BASE_URL}Downloads/HouseTcpByCandidateByVoteTypeDownload-{ELECTION_CODE}.csv"
get_file_from_url(url=URL, output_directory=INPUT_DIRECTORY)

# get tpp by pp
URL = f"{BASE_URL}Downloads/HouseTppByPollingPlaceDownload-{ELECTION_CODE}.csv"
get_file_from_url(url=URL, output_directory=INPUT_DIRECTORY)

# get tpp by vote type
URL = f"{BASE_URL}Downloads/HouseTppByDivisionByVoteTypeDownload-{ELECTION_CODE}.csv"
get_file_from_url(url=URL, output_directory=INPUT_DIRECTORY)
