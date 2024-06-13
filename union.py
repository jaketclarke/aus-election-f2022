import os
import geopandas as gpd
import pandas as pd

# Load the two shapefiles
sa1 = f"input{os.sep}sa1_2016_test_cutdown_wgs84.shp"
boundary = f"input{os.sep}2021_elb_cutdown_wgs84.shp"
output = f"output{os.sep}area_overlap.csv"
output2 = f"output{os.sep}area_with_pc.csv"


sa1_id_name = "SA1_7DIG16"
district_id_name = "Elect_div"


sa1_data = gpd.read_file(sa1)


boundary_data = gpd.read_file(boundary)

# Reproject to a projected CRS
sa1_data = sa1_data.to_crs(epsg=sa1_data.estimate_utm_crs().to_epsg())
boundary_data = boundary_data.to_crs(epsg=boundary_data.estimate_utm_crs().to_epsg())

# Spatial join to find overlapping areas
overlap_data = gpd.overlay(sa1_data, boundary_data, how="intersection")

# Calculate area of overlapping geometries
overlap_data["overlap_area"] = overlap_data.area

# Select columns for output
output_columns = [sa1_id_name, district_id_name, "overlap_area"]

overlap_data_sorted = overlap_data.sort_values(by=sa1_id_name)


# Write to CSV
overlap_data_sorted[output_columns].to_csv(output, index=False, encoding="utf8")


df = pd.read_csv(output)

total_overlap_area = df.groupby("SA1_7DIG16")["overlap_area"].sum().reset_index()
total_overlap_area.columns = [
    "SA1_7DIG16",
    "total_overlap_area",
]  # Rename the columns for clarity

# Merge total overlap area back into the original DataFrame
df = pd.merge(df, total_overlap_area, on="SA1_7DIG16", how="left")
df["proportion"] = df["overlap_area"] / df["total_overlap_area"]

print(df)

df = df[df["proportion"] >= 0.02]
df["proportion"] = round(df["overlap_area"] / df["total_overlap_area"], 4)


df.to_csv(output2, index=False, encoding="utf8")
