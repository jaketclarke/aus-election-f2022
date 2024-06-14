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
PRIMARY_DATA_FILEPATH = f"{OUTPUT_DIRECTORY}{os.sep}f2022_fp_by_polling_place_pc.csv"
PRIMARY_VOTETYPE_DATA_FILEPATH = (
    f"{OUTPUT_DIRECTORY}{os.sep}f2022_fp_by_vote_type_pc.csv"
)
SA1_DATA = f"{INPUT_DIRECTORY}{os.sep}2022-federal-election-votes-sa1.csv"

# make matching df
df_primary_data = pd.read_csv(PRIMARY_DATA_FILEPATH)
df_other_data = pd.read_csv(PRIMARY_VOTETYPE_DATA_FILEPATH)
df_sa1 = pd.read_csv(SA1_DATA)

# filter out the bits of the other data file we want
df_other_data = df_other_data[
    df_other_data["VoteType"].isin(
        ["AbsentVotes", "PostalVotes", "ProvisionalVotes", "PrePollVotes"]
    )
]

# rename to match the format of the sa1 data
replacements = {
    "AbsentVotes": "Absent",
    "PostalVotes": "Postal",
    "PrePollVotes": "Pre-Poll",
    "ProvisionalVotes": "Provisional",
}

df_other_data["VoteType"] = df_other_data["VoteType"].replace(replacements)
df_other_data.rename(columns={"VoteType": "Lookup"}, inplace=True)
df_primary_data.rename(columns={"PollingPlaceID": "Lookup"}, inplace=True)

# merge together
df_data = pd.concat([df_other_data, df_primary_data], ignore_index=True)
df_data = df_data.sort_values(["DivisionNm", "Lookup"])

# cutdown sa1 data
df_sa1 = df_sa1[["div_nm", "ccd_id", "pp_id", "pp_nm", "votes"]]

# rename columns
df_sa1.rename(
    columns={
        "div_nm": "DivisionNm",
        "pp_id": "PollingPlaceID",
        "pp_nm": "PollingPlace",
        "ccd_id": "SA1_2016",
        "votes": "SA1_Total_Votes",
    },
    inplace=True,
)

# add lookup
df_sa1["Lookup"] = df_sa1.apply(
    lambda row: (
        row["PollingPlaceID"] if row["PollingPlaceID"] != 0 else row["PollingPlace"]
    ),
    axis=1,
)
df_sa1.insert(2, "Lookup", df_sa1.pop("Lookup"))
df_sa1 = df_sa1.sort_values(["DivisionNm", "SA1_2016", "Lookup"])
# df_sa1.to_csv("sa1.csv", index=False)
print(df_sa1)

df_sa1_with_vote_data = pd.merge(
    left=df_sa1, right=df_data, how="left", on=["DivisionNm", "Lookup"]
)
# df_sa1_with_vote_data.to_csv("df_sa1_with_vote_data.csv", index=False)

print(df_sa1_with_vote_data)

# multiply out
vote_columns = [
    "fp_alp_f2022_pc",
    "fp_grn_f2022_pc",
    "fp_ind_f2022_pc",
    "fp_inf_f2022_pc",
    "fp_lnp_f2022_pc",
    "fp_oth_f2022_pc",
    "fp_phon_f2022_pc",
    "fp_uap_f2022_pc",
    "fp_inf_f2022_pc_tot",
]
# column_to_multiply_by = "SA1_Total_Votes"

for column in vote_columns:
    df_sa1_with_vote_data[column] = (
        df_sa1_with_vote_data[column] * df_sa1_with_vote_data["SA1_Total_Votes"]
    )

# df_sa1_with_vote_data.to_csv("df_sa1_with_vote_data_multiplied.csv", index=False)


print(df_sa1_with_vote_data)
# group by sa1

# Group by 'SA1_2016' and calculate the sum of each specified column

df_sa1_grouped = df_sa1_with_vote_data.groupby(by="SA1_2016")[vote_columns].sum()
# df_sa1_grouped.to_csv("df_sa1_with_vote_data_multiplied_grouped.csv")

df_sa1_totals = (
    df_sa1_with_vote_data.groupby(by="SA1_2016")["SA1_Total_Votes"].sum().reset_index()
)
print(df_sa1_totals)

df_output = pd.merge(
    left=df_sa1_grouped, right=df_sa1_totals, on="SA1_2016", how="left"
)


for column in vote_columns:
    df_output[column] = df_output[column] / df_output["SA1_Total_Votes"]

df_other_data.rename(columns={"SA1_Total_Votes": "fp_tot_f2022"}, inplace=True)

print(df_output)
df_output.to_csv(
    f"{OUTPUT_DIRECTORY}{os.sep}f2022_fp_by_SA1_2016.csv", encoding="UTF8", index=False
)
