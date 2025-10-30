import pandas as pd
import os
from datetime import datetime

# Getting file directory
base_dir = os.getcwd()

# Load sample Transtats data
metar_sample = pd.concat([
    pd.read_csv(base_dir+"\\..\\metar_data\\KBNA_201"+str(i)+".csv",
                    low_memory=False) # to ignore mixed type on certain columns
    for i in range(3,9)
], 
ignore_index=True)

# NULLS WND - 999,...
metar_sample = metar_sample[metar_sample["WND"].str[:3] != "999"]

# NULLS CIG - 99999,...
metar_sample = metar_sample[metar_sample["CIG"].str[:5] != "99999"]

# NULLS VIS - 999999,...
metar_sample = metar_sample[metar_sample["VIS"].str[:5] != "999999"]

# NULLS TMP - +9999,9
metar_sample = metar_sample[metar_sample["TMP"].str[:5] != "+9999"]

# NULLS DEW - +9999,9
metar_sample = metar_sample[metar_sample["DEW"].str[:5] != "+9999"]

# NULLS SLP - 99999,9
metar_sample = metar_sample[metar_sample["SLP"].str[:5] != "99999"]


# Adding datetime date
metar_sample["UTC_DATE"] = [
    datetime.strptime(row["DATE"], "%Y-%m-%dT%H:%M:%S")
    for _, row in metar_sample.iterrows()
]

# Adding wind speed & wind direction column
metar_sample["WND_SPEED"] = [
    int(row["WND"].split(",")[3])
    for _, row in metar_sample.iterrows()
]

metar_sample["WND_DIR"] = [
    int(row["WND"].split(",")[0])
    for _, row in metar_sample.iterrows()
]

# Adding ceiling & visibility
metar_sample["CEILING"] = [
    int(row["CIG"].split(",")[0])
    for _, row in metar_sample.iterrows()
]
metar_sample["VISIBILITY"] = [
    int(row["VIS"].split(",")[0])
    for _, row in metar_sample.iterrows()
]

# Adding temp & dew_pnt & pressure
metar_sample["TEMP"] = [
    int(row["TMP"].split(",")[0])
    for _, row in metar_sample.iterrows()
]
metar_sample["DEW_PNT"] = [
    int(row["DEW"].split(",")[0])
    for _, row in metar_sample.iterrows()
]
metar_sample["PRESSURE"] = [
    int(row["SLP"].split(",")[0])
    for _, row in metar_sample.iterrows()
]

# Copying columns from tnst_sample to tnst_dt
metar_dt = metar_sample.rename(columns={
    "UTC_DATE": "date",
    "WND_DIR": "wind_dir",
    "WND_SPEED": "wind_speed",
    "CEILING": "ceiling",
    "VISIBILITY": "visibility",
    "TEMP": "temp",
    "DEW_PNT": "dew_pnt",
    "PRESSURE": "pressure",
})[["date", "wind_dir", "wind_speed", "ceiling", "visibility", "temp", "dew_pnt", "pressure"]].copy()
metar_dt.to_csv("metar_dt.csv", index=False)
