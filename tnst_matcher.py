import pandas as pd
import os
from datetime import datetime
import numpy as np

# Getting file directory
base_dir = os.getcwd()

# Getting csv's
tnst = pd.read_csv(base_dir+"\\tnst_dt.csv")
metar = pd.read_csv(base_dir+"\\metar_dt.csv")

# Convert date columns to datetime
tnst["date"] = pd.to_datetime(tnst["date"], utc=True)
metar["date"] = pd.to_datetime(metar["date"], utc=True)

# Sort both by date (required for merge_asof)
tnst = tnst.sort_values("date")
metar = metar.sort_values("date")

# Merge METAR features into TNST by closest previous METAR time
# 'direction="nearest"' uses the nearest time (can also use 'backward' or 'forward')
tnst_matched = pd.merge_asof(
    tnst,
    metar,
    on="date",
    direction="nearest"  # can also specify tolerance=pd.Timedelta("1h") if needed
)

tnst_matched.to_csv("tnst_matched.csv", index=False)

print(tnst_matched.head())