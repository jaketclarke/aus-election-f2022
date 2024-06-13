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

# load env vars
load_dotenv()

# logging
logging.basicConfig()
DEBUG_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")
print(f"debug level: {DEBUG_LEVEL}")
logging.basicConfig(level=DEBUG_LEVEL.upper())

# read data
sa1_weights_filepath = f"{OUTPUT_DIRECTORY}{os.sep}area_with_pc.csv"
df_sa1_weights = pd.read_csv(sa1_weights_filepath)

# read tcp
tcp_by_sa1_filepath = f"{OUTPUT_DIRECTORY}{os.sep}f2022_tcp_by_SA1_2016.csv"

df_tcp = pd.read_csv(tcp_by_sa1_filepath)


df = pd.merge(left=df_sa1_weights, right=df_tcp, on="SA1_2016", how="left")

print(df)
df["weight"] = df["proportion"] * df["SA1_Total_Votes"]

columns = [
    "tcp_alp_f2022_pc_tot",
    "tcp_grn_f2022_pc_tot",
    "tcp_ind_f2022_pc_tot",
    "tcp_lnp_f2022_pc_tot",
    "tcp_oth_f2022_pc_tot",
]
for col in columns:
    df[col] = (df[col] * df["weight"]).round(4)

working_output_filepath = f"{OUTPUT_DIRECTORY}{os.sep}tcp_long.csv"
df.to_csv(working_output_filepath, encoding="utf8", index=False)


df_results = df.groupby(by="Division").sum().reset_index()
df_results.drop(
    ["SA1_2016", "overlap_area", "total_overlap_area", "proportion", 'SA1_Total_Votes'],
    axis=1,
    inplace=True,
)
print(df_results)

for col in columns:
    df_results[col.replace('_pc_tot','')] = df_results[col]
    df_results[col.replace('_pc_tot','_pc')] = (df_results[col] / df_results["weight"]).round(4)
    df_results.drop([col], axis=1, inplace=True)

df_results.rename(columns={"weight": "total_f2022"}, inplace=True)

output_filepath = f"{OUTPUT_DIRECTORY}{os.sep}tcp_by_division.csv"
df_results.to_csv(output_filepath, encoding="utf8", index=False)

