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
#! todo - this should be repeatable by election if we vary that id below
DATA_FILEPATH = (
    f"{INPUT_DIRECTORY}{os.sep}HouseTcpByCandidateByPollingPlaceDownload-27966.csv"
)
DATA_OUTPUT_FILEPATH = f"{OUTPUT_DIRECTORY}{os.sep}f2022_tcp_by_polling_place.csv"

DATA_OUTPUT_FILEPATH_PIVOT = (
    f"{OUTPUT_DIRECTORY}{os.sep}f2022_tcp_by_polling_place_pivot.csv"
)

df_input = pd.read_csv(DATA_FILEPATH, encoding="UTF8", skiprows=1)


# # informal ballots have the position 999
# #! todo make this a method - repeated logic
# todo skip if tcp
# # df_formal = df_input[df_input["BallotPosition"] != 999]
df_formal = df_input

# add totals
# todo skip if tcp
# df_formal["TotalFormalVotes"] = df_formal.groupby("PollingPlaceID")[
#     "OrdinaryVotes"
# ].transform("sum")

df_input["TotalVotes"] = df_input.groupby("PollingPlaceID")["OrdinaryVotes"].transform(
    "sum"
)


# todo skip if tcp
# df_formal = df_formal[["PollingPlaceID", "CandidateID", "TotalFormalVotes"]]


# todo skip if tcp
# merge results
# df_results = pd.merge(
#     left=df_input, right=df_formal, how="left", on=["PollingPlaceID", "CandidateID"]
# )
df_results = df_input

# add proportions
df_results["TotalVotePc"] = df_results["OrdinaryVotes"] / df_results["TotalVotes"]

# todo skip if tcp
# df_results["FormalVotePc"] = (
#     df_results["OrdinaryVotes"] / df_results["TotalFormalVotes"]
# )

# output data
df_results.to_csv(DATA_OUTPUT_FILEPATH, encoding="UTF8", index=False)

# reformat to party groupings
df_parties = pd.read_csv("party_lookup.csv")
df_parties = df_parties[["PartyAb", "PartyGrp"]]

# todo add FormalVotePc to list if primary
df_cutdown = df_results[["DivisionNm", "PollingPlaceID", "PartyAb", "TotalVotePc"]]

df_cutdown["PartyAb"] = df_results["PartyAb"].fillna("INF")

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
        f"tcp_{col.lower()}_f2022_pc_tot"
        if col != ("DivisionNm", "") and col != ("PollingPlaceID", "")
        else col
    )
    for col in df_pc_total.columns
]

# todo skip if not primary
# pivot formal percentages
# df_pc_formal = df_merge.pivot_table(
#     index=["DivisionNm", "PollingPlaceID"],
#     columns="PartyGrp",
#     values="FormalVotePc",
#     aggfunc="sum",
#     fill_value=0,
# )

# df_pc_formal.columns = [
#     (
#         f"fp_{col.lower()}_f2022_pc"
#         if col != ("DivisionNm", "") and col != ("PollingPlaceID", "")
#         else col
#     )
#     for col in df_pc_formal.columns
# ]

# # put informal as pc of total on the formal vote df
# df_pc_total_cutdown = df_pc_total.reset_index()[
#     ["DivisionNm", "PollingPlaceID", "fp_inf_f2022_pc_tot"]
# ]

# df_output = pd.merge(
#     left=df_pc_formal,
#     right=df_pc_total_cutdown,
#     on=["DivisionNm", "PollingPlaceID"],
#     how="left",
# )
df_output = df_pc_total

# add total columns

# todo add , "TotalFormalVotes" if primary
df_primary_results_totals = df_results[["DivisionNm", "PollingPlaceID", "TotalVotes"]]
df_primary_results_totals = df_primary_results_totals.dropna()
df_primary_results_totals = df_primary_results_totals.drop_duplicates()

# todo remove if primary
# df_primary_results_totals["TotalFormalVotes"] = df_primary_results_totals[
#     "TotalFormalVotes"
# ].astype(int)

# todo add , "TotalFormalVotes": "fp_f2022_tot_formal" below if primary
df_primary_results_totals.rename(
    columns={"TotalVotes": "tcp_f2022_tot"},
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
    f"{OUTPUT_DIRECTORY}{os.sep}f2022_tcp_by_polling_place_pc.csv",
    encoding="UTF8",
    index=False,
)

# df_pc_formal = df_pc_formal.round(4)
# df_pc_formal.to_csv(
#     f"{OUTPUT_DIRECTORY}{os.sep}f2022_fp_by_polling_place_pc_formal.csv",
#     encoding="UTF8",
# )

df_pc_total = df_pc_total.round(4)
df_pc_total.to_csv(
    f"{OUTPUT_DIRECTORY}{os.sep}f2022_tcp_by_polling_place_pc_total.csv",
    encoding="UTF8",
)
