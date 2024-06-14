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
VOTETYPE_DATA_FILEPATH = (
    f"{INPUT_DIRECTORY}{os.sep}HouseTcpByCandidateByVoteTypeDownload-27966.csv"
)
VOTETYPE_DATA_OUTPUT_FILEPATH = f"{OUTPUT_DIRECTORY}{os.sep}f2022_tcp_by_vote_type.csv"

df = pd.read_csv(VOTETYPE_DATA_FILEPATH, skiprows=1)

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
df_raw_output = pd.merge(
    left=df_melted,
    right=df_formal,
    how="left",
    on=["VoteType", "DivisionID"],
)

# add proportions
df_raw_output["TotalVotePc"] = df_raw_output["Votes"] / df_raw_output["TotalVotes"]

df_raw_output["FormalVotePc"] = (
    df_raw_output["Votes"] / df_raw_output["TotalFormalVotes"]
)

# sort
df_raw_output.sort_values(
    by=["StateAb", "DivisionID", "CandidateID", "VoteType"], inplace=True
)

# output data
df_raw_output.to_csv(VOTETYPE_DATA_OUTPUT_FILEPATH, encoding="UTF8", index=False)


# reformat to party groupings
df_parties = pd.read_csv("party_lookup.csv")
df_parties = df_parties[["PartyAb", "PartyGrp"]]

df_cutdown = df_raw_output[
    ["DivisionNm", "VoteType", "PartyAb", "TotalVotePc", "FormalVotePc"]
]
df_cutdown["PartyAb"] = df_raw_output["PartyAb"].fillna("INF")

df_merge = pd.merge(left=df_cutdown, right=df_parties, how="left", on="PartyAb")

# pivot total percentages
df_pc_total = df_merge.pivot_table(
    index=["DivisionNm", "VoteType"],
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

# # pivot formal percentages
# df_pc_formal = df_merge.pivot_table(
#     index=["DivisionNm", "VoteType"],
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
#     ["DivisionNm", "VoteType", "fp_inf_f2022_pc_tot"]
# ]

# df_output = pd.merge(
#     left=df_pc_formal,
#     right=df_pc_total_cutdown,
#     on=["DivisionNm", "VoteType"],
#     how="left",
# )
df_output = df_pc_total

# add total columns
df_primary_results_totals = df_raw_output[["DivisionNm", "VoteType", "TotalVotes"]]
df_primary_results_totals = df_primary_results_totals.dropna()
df_primary_results_totals = df_primary_results_totals.drop_duplicates()


df_primary_results_totals.rename(
    columns={"TotalVotes": "tcp_f2022_tot"},
    inplace=True,
)

df_output = pd.merge(
    left=df_output,
    right=df_primary_results_totals,
    how="left",
    on=["DivisionNm", "VoteType"],
)

df_output = df_output.round(4)
df_output.to_csv(
    f"{OUTPUT_DIRECTORY}{os.sep}f2022_tcp_by_vote_type_pc.csv",
    encoding="UTF8",
    index=False,
)

df_pc_total = df_pc_total.round(4)
df_pc_total.to_csv(
    f"{OUTPUT_DIRECTORY}{os.sep}f2022_tcp_by_vote_type_pc_total.csv", encoding="UTF8"
)
