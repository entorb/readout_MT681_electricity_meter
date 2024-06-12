"""
Analyze the data from InfluxDB database.

meter vs. PV
"""

import time
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
import pandas as pd

from influx_creds import credentials_read as creds
from influx_helper import (
    connect,
    df_interpolate_missing_meter_data,
    df_interpolate_missing_pv_data,
    fetch_data_to_pd,
    time_to_datetime,
)

TZ_DE = ZoneInfo("Europe/Berlin")


def check_cache_file_available_and_recent(
    file_path: Path,
    max_age: int = 3500,
) -> bool:
    """Check if cache file exists and is recent."""
    cache_good = False
    if file_path.exists() and (time.time() - file_path.stat().st_mtime < max_age):
        cache_good = True
    return cache_good


def read_data_from_db_or_file(
    filename: str,
    query: str,
) -> pd.DataFrame:
    """Read data cache file if recent, else from DB."""
    if check_cache_file_available_and_recent(Path(filename), max_age=3 * 3600):
        print("Using cache file " + filename)
        df = pd.read_csv(filename, sep="\t", lineterminator="\n")
    else:
        df = fetch_data_to_pd(client, query)
        df.to_csv(
            filename,
            sep="\t",
            lineterminator="\n",
            index=False,
        )
    if "room" in df.columns:  # Shelly data
        df = df.drop(columns=["room"])

    df = time_to_datetime(df)
    return df


client = connect(creds)

# MT681 meter data
filename = "data-meter.tsv"
query = """
SELECT * FROM "tasmota_MT681"
WHERE time > '2024-06-01'
AND time > now() - 30d
--AND time > '2024-06-10'
--AND time < '2024-06-11'
"""
df_meter = read_data_from_db_or_file(filename, query)

# Shelly PV data
filename = "data-pv.tsv"
query = """
SELECT * FROM "Shelly3"
WHERE room='Balkon'
AND time > '2024-06-01'
AND time > now() - 30d
--AND time > '2024-06-10'
--AND time < '2024-06-11'
"""
df_pv = read_data_from_db_or_file(filename, query)

client.close()

# consumption per day
df = df_meter.reset_index()
df = (
    df[["time", "kWh_total_in", "kWh_total_out"]]
    .groupby([pd.Grouper(key="time", freq="D")])
    .agg({"kWh_total_in": "first", "kWh_total_out": "first"})
)
# delta
df["kWh_in"] = df["kWh_total_in"].shift(-1) - df["kWh_total_in"]
df["kWh_out"] = df["kWh_total_out"].shift(-1) - df["kWh_total_out"]
df_kwh_day = df[["kWh_in", "kWh_out"]]

df = df_pv.reset_index()
df = (
    df[["time", "kWh_total"]]
    .groupby([pd.Grouper(key="time", freq="D")])
    .agg({"kWh_total": "first"})
)
df["kWh_pv"] = df["kWh_total"].shift(-1) - df["kWh_total"]

df_kwh_day = pd.concat([df_kwh_day, df[["kWh_pv"]]], axis=1)

# kWh increase
# min = df_meter["kWh_total_in"].min()
# max = df_meter["kWh_total_in"].max()
# print(max - min)

# df_meter
# in 10s freq
# columns: 'kWh_total_in', 'kWh_total_out', 'watt'
# can have missing rows

df_meter = df_interpolate_missing_meter_data(df_meter).rename(columns={"watt": "meter"})

# in 1min freq
# columns: 'meter' (Watt), kWh_total_in, kWh_total_out
# no missing rows

# df_pv
# in 1min freq
# columns: 'kWh_total', 'watt_last' (average of last minute), 'watt_now'


df_pv = df_interpolate_missing_pv_data(df_pv).rename(columns={"watt_last": "pv"})
# in 1min freq
# columns: 'pv' (Watt)
# no missing rows

# concatenate df_meter and df_pv


df = pd.concat(
    [df_meter[["meter"]], df_pv[["pv"]]],
    axis=1,
)


df["sum"] = df["meter"] + df["pv"]

# df["sum"] = df["sum"].rolling(window=9, min_periods=1).mean()

# df["pv"] = df["pv"].rolling(window=9, min_periods=1).mean()

# sum = 0 if < 0
df["sum"] = df["sum"].clip(lower=0)

df["meter+"] = df["meter"].clip(lower=0)
# sum of "meter+"
print(df["meter+"].sum() / 1000 / 60)

df["saving"] = df[["sum", "pv"]].min(axis=1)

# convert Watt * min -> kWh per min

df["kWh_saved"] = df["saving"] / 60 / 1000

# Convert 1min freq to daily sum
df = df.reset_index()
df = (
    df[["time", "kWh_saved"]]
    .groupby([pd.Grouper(key="time", freq="D")])
    .agg({"kWh_saved": "sum"})
    .round(1)
)

df = pd.concat([df_kwh_day, df], axis=1)

# df[["meter", "pv", "sum", "saving"]].plot()
# df[["saving", "pv", "meter"]].plot()
plt.grid(axis="both")
df.plot()
# layout
plt.suptitle("Comparing kWh consumption, production and PV-savings")
plt.xlabel("")
plt.ylabel("kWh")

plt.grid(axis="both")
plt.tight_layout()
plt.savefig(fname="test.png", format="png")
plt.close()

df.to_csv(
    "test.tsv",
    sep="\t",
    lineterminator="\n",
    index=True,
)
