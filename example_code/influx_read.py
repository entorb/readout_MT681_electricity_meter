"""
Analyze the data from InfluxDB database.

meter vs. PV
"""

import time
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.pyplot as plt
import pandas as pd
from influxdb import InfluxDBClient

from influx_read_creds import credentials as creds

TZ_DE = ZoneInfo("Europe/Berlin")


def connect() -> InfluxDBClient:
    """Connect to DB."""
    client = InfluxDBClient(
        host=creds["host"],
        port=creds["port"],
        username=creds["user"],
        password=creds["password"],
    )
    client.switch_database(creds["database"])
    return client


def check_cache_file_available_and_recent(
    file_path: Path,
    max_age: int = 3500,
) -> bool:
    """Check if cache file exists and is recent."""
    cache_good = False
    if file_path.exists() and (time.time() - file_path.stat().st_mtime < max_age):
        cache_good = True
    return cache_good


def fetch_data_to_pd(query: str) -> pd.DataFrame:
    """Fetch data from DB into to Pandas DataFrame."""
    result = client.query(query)
    data = result.get_points()  # type: ignore
    df = pd.DataFrame(data)
    return df


def read_data_from_db_or_file(
    filename: str,
    query: str,
) -> pd.DataFrame:
    """Read data cache file if recent, else from DB."""
    if check_cache_file_available_and_recent(Path(filename), max_age=3 * 3600):
        print("Using cache file " + filename)
        df = pd.read_csv(filename, sep="\t", lineterminator="\n")
    else:
        df = fetch_data_to_pd(query)
        df.to_csv(
            filename,
            sep="\t",
            lineterminator="\n",
            index=False,
        )
    if "room" in df.columns:  # Shelly data
        df = df.drop(columns=["room"])

    df["time"] = pd.to_datetime(df["time"])

    # convert UTC to local time
    df["time"] = (
        df["time"]
        # .dt.tz_localize("utc")
        .dt.tz_convert(tz="Europe/Berlin")
        # drop timezone info, since Excel can not handle it
        .dt.tz_localize(None)
    )

    df = df.set_index(["time"])
    # df.index = pd.to_datetime(df.index)
    # print(df)
    return df


client = connect()

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

# df_meter
# in 15s freq
# columns: 'kWh_total_in', 'kWh_total_out', 'watt'
# can have missing rows

# kWh increase
# min = df_meter["kWh_total_in"].min()
# max = df_meter["kWh_total_in"].max()
# print(max - min)

# df_pv
# in 1min freq
# columns: 'kWh_total', 'watt_last' (average of last minute), 'watt_now'

# df_meter : filling of gaps
df = df_meter.reset_index()

# calc deltas to previous rows
df["Delta_time"] = (
    df["time"] - df["time"].shift(1)
).dt.total_seconds() / 60  # min-> sec
df["Delta_kWh_total_in"] = (df["kWh_total_in"] - df["kWh_total_in"].shift(1)) / df[
    "Delta_time"
]
df["Delta_kWh_total_out"] = (df["kWh_total_out"] - df["kWh_total_out"].shift(1)) / df[
    "Delta_time"
]
# calc watt from deltas, used later for filling gaps
df["watt_calc"] = (df["Delta_kWh_total_in"] - df["Delta_kWh_total_out"]) * 1000 * 60

# Convert from 15s freq to 1min freq, using mean
df = (
    df[["time", "watt", "watt_calc"]]
    .groupby([pd.Grouper(key="time", freq="1min")])
    .agg({"watt": "mean", "watt_calc": "mean"})
)

# print where it is NaN
print(df[df["watt"].isna()])

# fill in gaps by backwards filling (bfill)
df["watt_calc"] = df["watt_calc"].bfill()

# overwrite df["watt"] with df["watt_calc"] where df["watt"].isna()
df["watt"] = df["watt"].fillna(df["watt_calc"])

df = df.drop(columns=["watt_calc"])

df = df.rename(columns={"watt": "meter"})

df_meter = df
# in 1min freq
# columns: 'meter' (Watt)
# no missing rows


#
# PV
#
# df_pv calc delta
df = df_pv.reset_index()
# calc deltas to previous rows
df["Delta_time"] = (
    df["time"] - df["time"].shift(1)
).dt.total_seconds() / 60  # min-> sec
df["Delta_kWh_total"] = (df["kWh_total"] - df["kWh_total"].shift(1)) / df["Delta_time"]

# calc watt from deltas, used later for filling gaps
df["watt_calc"] = df["Delta_kWh_total"] * 1000 * 60
# moving average over 5min
# to prevent steps of 60W whenever the kWh increases by 0.001 (min accuracy)
df["watt_calc"] = df["watt_calc"].rolling(5).mean()


# add missing times
df = (
    df[["time", "watt_last", "watt_calc"]]
    .groupby([pd.Grouper(key="time", freq="1min")])
    .agg({"watt_last": "mean", "watt_calc": "mean"})
)

# fill in gaps by backwards filling (bfill)
df["watt_calc"] = df["watt_calc"].bfill()

# overwrite df["watt"] with df["watt_calc"] where df["watt"].isna()
df["watt_last"] = df["watt_last"].fillna(df["watt_calc"])


df = df.drop(columns=["watt_calc"])

df = df.rename(columns={"watt_last": "pv"})

df_pv = df
# in 1min freq
# columns: 'pv' (Watt)
# no missing rows

# concatenate df_meter and df_pv


df = pd.concat(
    [df_meter[["meter"]], df_pv[["pv"]]],
    axis=1,
)


df["sum"] = df["meter"] + df["pv"]

# df["sum"] = df["sum"].rolling(4).mean()

# df["pv"] = df["pv"].rolling(4).mean()


# sum = 0 if < 0
df["sum"] = df["sum"].clip(lower=0)

df["meter+"] = df["meter"].clip(lower=0)
# sum of "meter+"
print(df["meter+"].sum() / 1000 / 60)

df["saving"] = df[["sum", "pv"]].min(axis=1)

# convert Watt * min -> kWh

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
