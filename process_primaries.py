import os
import logging
from dotenv import load_dotenv
from helpers import ensure_env_var_exists
import pandas as pd
import numpy as np

# settings
INPUT_DIRECTORY = "input"
WORKING_DIRECTORY = "working"

# load env vars
load_dotenv()

# logging
logging.basicConfig()
DEBUG_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")
ELECTION_CODE = ensure_env_var_exists("ELECTION_CODE")

print(f"debug level: {DEBUG_LEVEL}, running for election {ELECTION_CODE}")
logging.basicConfig(level=DEBUG_LEVEL.upper())


def process_primaries():

    # add proportions to primary data
    primaryDataFileName = f"{ELECTION_CODE}_primaries.csv"
    primaryDataPath = f"{WORKING_DIRECTORY}{os.sep}{primaryDataFileName}"
    primaryData = pd.read_csv(primaryDataPath)

    # work out total column
    # joint booths have different PollingPlaceIDs
    primaryTotal = (
        primaryData.groupby(["PollingPlaceID"])
        .sum()["OrdinaryVotes"]
        .reset_index("PollingPlaceID")
    )
    primaryTotal.rename({"OrdinaryVotes": "Total Primary Votes"}, axis=1, inplace=True)

    primaryData = primaryData.merge(primaryTotal, on="PollingPlaceID")

    # work out total formal
    primaryDataFormal = primaryData[primaryData.PartyNm != "Informal"]
    primaryTotalFormal = (
        primaryDataFormal.groupby(["PollingPlaceID"])
        .sum()["OrdinaryVotes"]
        .reset_index("PollingPlaceID")
    )
    primaryTotalFormal.rename(
        {"OrdinaryVotes": "Total Formal Primary Votes"}, axis=1, inplace=True
    )

    primaryData = primaryData.merge(primaryTotalFormal, on="PollingPlaceID")

    # Proportions calc
    primaryData["OrdinaryVotesPcTotal"] = primaryData["OrdinaryVotes"].div(
        primaryData["Total Primary Votes"].values
    )
    primaryData["OrdinaryVotesPcFormalTotal"] = primaryData["OrdinaryVotes"].div(
        primaryData["Total Formal Primary Votes"].values
    )

    # What we want is informal as % total, votes % total formal
    primaryData["OrdinaryVotesPc"] = np.where(
        primaryData["PartyNm"] == "Informal",
        primaryData["OrdinaryVotes"] / primaryData["Total Primary Votes"],
        primaryData["OrdinaryVotes"] / primaryData["Total Formal Primary Votes"],
    )

    # output this file
    primaryDataOutputFileName = f"{ELECTION_CODE}_primaries_with_proportions.csv"
    primaryDataOutputPath = f"{WORKING_DIRECTORY}{os.sep}{primaryDataOutputFileName}"
    primaryData.to_csv(primaryDataOutputPath, index=False)

    # pivot data
    # List of base fields to pivot
    fields_to_pivot = [
        "OrdinaryVotes",
        "Swing",
        "Total Primary Votes",
        "Total Formal Primary Votes",
        "OrdinaryVotesPcTotal",
        "OrdinaryVotesPcFormalTotal",
        "OrdinaryVotesPc",
    ]

    # Define the index for the pivot table (location identifiers)
    group_by_keys = [
        "StateAb",
        "DivisionID",
        "DivisionNm",
        "PollingPlaceID",
        "PollingPlace",
    ]

    # Calculate total_formal_votes per location
    total_formal_votes_df = (
        primaryData.groupby(group_by_keys)["Total Formal Primary Votes"]
        .sum()
        .reset_index(name="total_formal_votes")
    )

    # Calculate total_votes per location
    total_votes_df = (
        primaryData.groupby(group_by_keys)["Total Primary Votes"]
        .sum()
        .reset_index(name="total_votes")
    )

    # Perform the pivot operation
    pivot_df = primaryData.pivot_table(
        index=group_by_keys, columns="PartyAb", values=fields_to_pivot, aggfunc="sum"
    )

    # Create new single-level column names
    new_columns = []
    for col in pivot_df.columns:
        if col[1] == "":  # For the index columns (after reset_index)
            value = col[0].replace(" ", "_").lower()
            new_columns.append(value)
        else:
            new_col_name = f"{col[1]}_{col[0]}".replace(" ", "_").lower()
            new_columns.append(new_col_name)

    # Assign the new column names
    pivot_df.columns = new_columns

    # this will make it output a regular spreadsheet
    pivot_df.reset_index(inplace=True)

    # add totals
    pivot_df = pd.merge(pivot_df, total_formal_votes_df, on=group_by_keys, how="left")
    pivot_df = pd.merge(pivot_df, total_votes_df, on=group_by_keys, how="left")

    primaryDataOutputFileNamePivoted = (
        f"{ELECTION_CODE}_primaries_with_proportions_pivot.csv"
    )
    primaryDataOutputPathPivoted = (
        f"{WORKING_DIRECTORY}{os.sep}{primaryDataOutputFileNamePivoted}"
    )
    pivot_df.to_csv(primaryDataOutputPathPivoted, index=False)


process_primaries()
