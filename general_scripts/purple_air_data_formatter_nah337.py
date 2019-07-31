''' Script that parses files formatted by PurpleAir

Submitted by Nick Hespe, nah337, 2019/07/22

Data was ingested into individual csvs from the purpleAir web ui

Files needed heavy parsing that I couldnt do in Scala :/

After parsing, the csv was scp'ed into my nyu account then:
 HDFS -dfs put purple_air_data.csv /scratch/nah337/project/purple_air_data/
'''


import logging
import re
from os import listdir
from os.path import isfile, join

import pandas as pd

MYPATH = "purple_air_data/"

def parse_filename(text_file):
    """ Data is in format Location (lat long) info_type start_period end_period """
    out = []
    text_file = re.sub("\([0-9]", "((", text_file)
    remove_csv = text_file[:-4]
    first_split = remove_csv.split("((")
    out.append(first_split[0])
    out.extend(first_split[1].split(" "))
    return [re.sub(r"[)]", "", x.strip()) for x in out]

# Get Filenames
logging.info(f"Searching directory {MYPATH}")
filenames = [f for f in listdir(mypath) if isfile(join(mypath, f))]
filenames.remove("load_and_parse_data.ipynb")
filenames.remove(".DS_Store")
logging.info(f"Found {len(filenames)} Files in the directory")

# Parse Files
dfs = []
for filename in filenames:
    logging.info(f"Parsing file '{filename}' to dataframe")
    fil_info = parse_filename(filename)
    this_df = pd.read_csv(filename)
    this_df["location"] = fil_info[0]
    this_df["lat"] = fil_info[1]
    this_df["long"] = fil_info[2]
    this_df["type"] = fil_info[3]
    this_df["start_period"] = fil_info[4]
    this_df["end_period"] = fil_info[5]
    dfs.append(this_df)

# Concatenate all data into one DataFrame
output_frame = pd.concat(dfs, ignore_index=True)

# Rename columns to more programatic-friendly formats 
output_frame = output_frame.rename(columns={
    "0.3um/dl": "0_3um_dl",
    "0.5um/dl": "0_5um_dl",
    "1.0um/dl": "1_0um_dl",
    "10.0um/dl": "10_0um_dl",
    "2.5um/dl":"2_5um_dl",
    "5.0um/dl":"5_0um_dl" ,
    "Humidity_%": "Humidity_perc",
    "PM1.0_CF_1_ug/m3": "PM1_0_CF_1_ug_m3",
    "PM1.0_CF_ATM_ug/m3": "PM1_0_CF_ATM_ug_m3",
    "PM10.0_CF_ATM_ug/m3": "PM10_0_CF_ATM_ug_m3",
    "PM10_CF_1_ug/m3": "PM10_CF_1_ug_m3",
    "PM2.5_CF_1_ug/m3": "PM2_5_CF_1_ug_m3",
    "PM2.5_CF_ATM_ug/m3": "PM2_5_CF_ATM_ug_m3",
})

# Remove superfluous columns
del output_frame["--"]
del output_frame["Unnamed: 10"]
del output_frame["type"]

# Persist
output_frame.to_csv("purple_air_data_20190722.csv")
