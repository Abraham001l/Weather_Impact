import pandas as pd
import os
from timezonefinderL import TimezoneFinder
from datetime import datetime
from zoneinfo import ZoneInfo
import pytz

# Getting file directory
base_dir = os.getcwd()

# Creating instance of TimezoneFinder (using timezoneFinder light)
tf = TimezoneFinder()

# Load IATA to ICAO/Latitude/Longitude mapping
iata_icao = pd.read_csv(base_dir+"\\iata-icao.csv")
iata_to_icao = dict(zip(iata_icao["iata"], iata_icao["icao"]))
iata_to_latitude = dict(zip(iata_icao["iata"], iata_icao["latitude"]))
iata_to_longitude = dict(zip(iata_icao["iata"], iata_icao["longitude"]))

# Load sample Transtats data
tnst_sample = pd.concat([
    pd.read_csv(base_dir+"\\..\\transtats_data\\T_ONTIME_REPORTING_"+str(i)+".csv",
                     dtype={"CRS_DEP_TIME": str}, # changing dep to string to keep leading zeros
                     na_values={"WEATHER_DELAY": [0]}) # changing weather_delay 0 values to NaN
    for i in range(60,84)
], 
ignore_index=True)

# Remove rows with null values (those without weather delay)
tnst_sample.dropna(inplace=True) 
tnst_sample.reset_index(drop=True, inplace=True)

# Adding ICAO/Latitude/Longitude columns using maps
tnst_sample["ICAO"] = tnst_sample["ORIGIN"].map(iata_to_icao)
tnst_sample["LATITUDE"] = tnst_sample["ORIGIN"].map(iata_to_latitude)
tnst_sample["LONGITUDE"] = tnst_sample["ORIGIN"].map(iata_to_longitude)

# Remove rows with null values
tnst_sample.dropna(inplace=True) 
tnst_sample.reset_index(drop=True, inplace=True)


# Adding timezone and removing non K icao's (non US)
tnst_sample["LOCAL_TIMEZONE"] = [
    tf.timezone_at(lat=row["LATITUDE"], lng=row["LONGITUDE"])
    for _, row in tnst_sample.iterrows()]
tnst_sample = tnst_sample[tnst_sample["ICAO"].str.startswith("K", na=False)] # inner indexing returns list of true and false (na=False treats nulls as false)

# Building timezone object dictionary (for efficiency)
unique_tzs = tnst_sample["LOCAL_TIMEZONE"].unique()
tz_dict = {tz: ZoneInfo(tz) for tz in unique_tzs}

# Generating UTC timezone column
tnst_sample["UTC_TIMEZONE"] = [
    datetime.strptime(row["FL_DATE"].split()[0]+" "+row["CRS_DEP_TIME"][0:2]+":"+row["CRS_DEP_TIME"][2:], 
                      "%m/%d/%Y %H:%M").replace(tzinfo=tz_dict[row["LOCAL_TIMEZONE"]]).astimezone(pytz.UTC)
                      # assigning timzone and converting to UTC
    for _, row in tnst_sample.iterrows()
]

# Extracting KBNA (60-83 : 2015-2016)
tnst_sample = tnst_sample[tnst_sample["ICAO"]=="KBNA"]

# Remove rows with null values
tnst_sample.dropna(inplace=True)
tnst_sample.reset_index(drop=True, inplace=True)


# Copying columns from tnst_sample to tnst_dt
tnst_dt = tnst_sample.rename(columns={
    "UTC_TIMEZONE": "date",
    "ICAO": "icao",
    "WEATHER_DELAY": "weather_delay"
})[["date", "icao", "weather_delay"]].copy()

tnst_dt.to_csv("tnst_dt.csv", index=False)

