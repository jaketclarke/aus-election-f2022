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

PRIMARY_DATA_OUTPUT_FILEPATH_PIVOT = (
    f"{OUTPUT_DIRECTORY}{os.sep}f2022_fp_by_polling_place_pivot.csv"
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


# reformat to party groupings
df_parties = pd.read_csv("party_lookup.csv")
df_parties = df_parties[["PartyAb", "PartyGrp"]]

df_cutdown = df_primary_results[
    ["DivisionNm", "PollingPlaceID", "PartyAb", "TotalVotePc", "FormalVotePc"]
]
df_cutdown["PartyAb"] = df_primary_results["PartyAb"].fillna("INF")

df_merge = pd.merge(left=df_cutdown, right=df_parties, how="left", on="PartyAb")


# pivot total percentages
df_pc_total = df_merge.pivot_table(
    index=["DivisionNm", "PollingPlaceID"],
    columns="PartyGrp",
    values="TotalVotePc",
    aggfunc="sum",
    fill_value=0,
)

df_pc_total.columns = [
    (
        f"fp_{col.lower()}_f2022_pc_tot"
        if col != ("DivisionNm", "") and col != ("PollingPlaceID", "")
        else col
    )
    for col in df_pc_total.columns
]

# pivot formal percentages
df_pc_formal = df_merge.pivot_table(
    index=["DivisionNm", "PollingPlaceID"],
    columns="PartyGrp",
    values="FormalVotePc",
    aggfunc="sum",
    fill_value=0,
)

df_pc_formal.columns = [
    (
        f"fp_{col.lower()}_f2022_pc"
        if col != ("DivisionNm", "") and col != ("PollingPlaceID", "")
        else col
    )
    for col in df_pc_formal.columns
]

# put informal as pc of total on the formal vote df
df_pc_total_cutdown = df_pc_total.reset_index()[
    ["DivisionNm", "PollingPlaceID", "fp_inf_f2022_pc_tot"]
]

df_output = pd.merge(
    left=df_pc_formal,
    right=df_pc_total_cutdown,
    on=["DivisionNm", "PollingPlaceID"],
    how="left",
)

# add total columns

df_primary_results_totals = df_primary_results[
    ["DivisionNm", "PollingPlaceID", "TotalVotes", "TotalFormalVotes"]
]
df_primary_results_totals = df_primary_results_totals.dropna()
df_primary_results_totals = df_primary_results_totals.drop_duplicates()

df_primary_results_totals["TotalFormalVotes"] = df_primary_results_totals[
    "TotalFormalVotes"
].astype(int)

df_primary_results_totals.rename(
    columns={"TotalVotes": "fp_f2022_tot", "TotalFormalVotes": "fp_f2022_tot_formal"},
    inplace=True,
)

df_output = pd.merge(
    left=df_output,
    right=df_primary_results_totals,
    how="left",
    on=["DivisionNm", "PollingPlaceID"],
)

df_output = df_output.round(4)
df_output.to_csv(
    f"{OUTPUT_DIRECTORY}{os.sep}f2022_fp_by_polling_place_pc.csv",
    encoding="UTF8",
    index=False,
)

df_pc_formal = df_pc_formal.round(4)
df_pc_formal.to_csv(
    f"{OUTPUT_DIRECTORY}{os.sep}f2022_fp_by_polling_place_pc_formal.csv",
    encoding="UTF8",
)

df_pc_total = df_pc_total.round(4)
df_pc_total.to_csv(
    f"{OUTPUT_DIRECTORY}{os.sep}f2022_fp_by_polling_place_pc_total.csv", encoding="UTF8"
)
