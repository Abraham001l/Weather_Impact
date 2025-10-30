import pandas as pd
import os
from timezonefinderL import TimezoneFinder
from datetime import datetime
from zoneinfo import ZoneInfo
import pytz
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from scipy import stats

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
tnst_data = pd.concat([
    pd.read_csv(base_dir+"\\..\\transtats_data\\T_ONTIME_REPORTING_"+str(i)+".csv",
                     dtype={"CRS_DEP_TIME": str}) # changing dep to string to keep leading zeros
    for i in range(36,108) # [36-83] 2013-2016 Training Data Set, [84-107] 2017-2018 Testing Data Set
], 
ignore_index=True)

# Fill NA values in WEATHER_DELAY with 0
tnst_data["WEATHER_DELAY"] = tnst_data["WEATHER_DELAY"].fillna(0)

# Adding ICAO/Latitude/Longitude columns using maps
tnst_data["ICAO"] = tnst_data["ORIGIN"].map(iata_to_icao)
tnst_data["LATITUDE"] = tnst_data["ORIGIN"].map(iata_to_latitude)
tnst_data["LONGITUDE"] = tnst_data["ORIGIN"].map(iata_to_longitude)

tnst_data = tnst_data[tnst_data["ICAO"]=="KBNA"]
tnst_data.reset_index(drop=True, inplace=True)

# Adding timezone and removing non K icao's (non US)
tnst_data["LOCAL_TIMEZONE"] = tnst_data.apply(
    lambda row: tf.timezone_at(lat=row["LATITUDE"], lng=row["LONGITUDE"]),
    axis=1
)

# Building timezone object dictionary (for efficiency)
unique_tzs = tnst_data["LOCAL_TIMEZONE"].unique()
tz_dict = {tz: ZoneInfo(tz) for tz in unique_tzs}

# Generating UTC timezone column
tnst_data["UTC_TIMEZONE"] = tnst_data.apply(
    lambda row: datetime.strptime(row["FL_DATE"].split()[0]+" "+row["CRS_DEP_TIME"][0:2]+":"+row["CRS_DEP_TIME"][2:], 
                      "%m/%d/%Y %H:%M").replace(tzinfo=tz_dict[row["LOCAL_TIMEZONE"]]).astimezone(pytz.UTC),
    axis=1
)

# Routine Cleanup
tnst_data.dropna(inplace=True) 
tnst_data.reset_index(drop=True, inplace=True)

# Getting training data set
tnst_train = tnst_data[:int(len(tnst_data)*0.75)]

# Building distribution of split between flights
train_time_splits = (tnst_train["UTC_TIMEZONE"].diff().dt.total_seconds().fillna(0) / 60)[1:]

# Building distribution & scoring delays
mu = np.mean(train_time_splits)
sigma = np.std(train_time_splits)


# Function to calculate the average split by a certain window size
def calc_avg_window_split(series, window_size):
    window_avgs = []

    for i in range(window_size, len(series)):
        window_splits = [series.iloc[i-j] for j in range(window_size)]
        window_avgs.append(np.mean(window_splits))
    return (np.array(window_avgs)).mean()

# Finding the best window size
window = 2
cur_diff = -1
while True:
    # Current average for window size
    cur_window_avg = calc_avg_window_split(train_time_splits, window)

    # Calculate distance from baseline
    diff_from_baseline = abs(mu-cur_window_avg)
    print(window, diff_from_baseline)
    # Checking if best window size
    if cur_diff == -1:
        cur_diff = diff_from_baseline
        window += 1
        print("trying window:", window)
        continue
    elif diff_from_baseline < cur_diff:
        cur_diff = diff_from_baseline
        window += 1
        print("trying window:", window)
        continue
    if not (window < 20):
        break
    window -= 1 # the previous window size was the best one
    break
    
print("Best window size found:", window)

# Creating global time splits
time_splits = (tnst_data["UTC_TIMEZONE"].diff().dt.total_seconds().fillna(0) / 60)


# Create congestion score
tnst_data["CONGESTION_SCORE"] = time_splits.rolling(window=window, min_periods=window).mean()
print(tnst_data.head())

# Routine Cleanup
tnst_data.dropna(inplace=True) 
tnst_data.reset_index(drop=True, inplace=True)

# Copying columns from tnst_data to tnst_dt
tnst_dt = tnst_data.rename(columns={
    "UTC_TIMEZONE": "date",
    "ICAO": "icao",
    "WEATHER_DELAY": "weather_delay",
    "CONGESTION_SCORE": "congestion_score"
})[["date", "icao", "weather_delay", "congestion_score"]].copy()

print(tnst_dt.head())
tnst_dt.to_csv("tnst_dt.csv", index=False)

