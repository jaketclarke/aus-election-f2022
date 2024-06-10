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
PRIMARY_VOTETYPE_DATA_FILEPATH = (
    f"{INPUT_DIRECTORY}{os.sep}HouseFirstPrefsByCandidateByVoteTypeDownload-27966.csv"
)
PRIMARY_VOTETYPE_DATA_OUTPUT_FILEPATH = (
    f"{OUTPUT_DIRECTORY}{os.sep}f2022_fp_by_vote_type.csv"
)

df = pd.read_csv(PRIMARY_VOTETYPE_DATA_FILEPATH, skiprows=1)

# "unpivot" data

df_melted = pd.melt(
    df,
    id_vars=df.columns[:10],
    value_vars=[
        "OrdinaryVotes",
        "AbsentVotes",
        "ProvisionalVotes",
        "PrePollVotes",
        "PostalVotes",
        "TotalVotes",
        "Swing",
    ],
    var_name="VoteType",
    value_name="Votes",
)

print(df_melted)

# delete where swing
df_melted = df_melted[df_melted["VoteType"] != "Swing"]

# calculate formal total
df_formal = df_melted[df_melted["BallotPosition"] != 999]

print(df_formal)

# add totals
df_formal["TotalFormalVotes"] = df_formal.groupby(["DivisionID", "VoteType"])[
    "Votes"
].transform("sum")

df_melted["TotalVotes"] = df_melted.groupby(["DivisionID", "VoteType"])[
    "Votes"
].transform("sum")

df_formal = df_formal[["VoteType", "DivisionID", "TotalFormalVotes"]].drop_duplicates()

# merge results
df_output = pd.merge(
    left=df_melted,
    right=df_formal,
    how="left",
    on=["VoteType", "DivisionID"],
)

# add proportions
df_output["TotalVotePc"] = df_output["Votes"] / df_output["TotalVotes"]

df_output["FormalVotePc"] = df_output["Votes"] / df_output["TotalFormalVotes"]

# sort
df_output.sort_values(
    by=["StateAb", "DivisionID", "CandidateID", "VoteType"], inplace=True
)

# output data
df_output.to_csv(PRIMARY_VOTETYPE_DATA_OUTPUT_FILEPATH, encoding="UTF8", index=False)
