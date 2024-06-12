"""InfluxDB helper functions."""

import pandas as pd
from influxdb import DataFrameClient, InfluxDBClient


def connect(creds: dict) -> InfluxDBClient:
    """Connect to DB."""
    client = InfluxDBClient(
        host=creds["host"],
        port=creds["port"],
        username=creds["user"],
        password=creds["password"],
    )
    client.switch_database(creds["database"])
    return client


def connect_write_df(creds: dict) -> DataFrameClient:
    """Connect to DB."""
    client = DataFrameClient(
        host=creds["host"],
        port=creds["port"],
        username=creds["user"],
        password=creds["password"],
    )
    client.switch_database(creds["database"])
    return client


def fetch_data_to_pd(client: InfluxDBClient, query: str) -> pd.DataFrame:
    """Fetch data from DB into to Pandas DataFrame."""
    result = client.query(query)
    data = result.get_points()  # type: ignore
    df = pd.DataFrame(data)
    return df


def time_to_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Convert time column to datetime."""
    df["time"] = pd.to_datetime(df["time"])

    # convert UTC to local time
    df["time"] = (
        df["time"]
        # .dt.tz_localize("utc")
        .dt.tz_convert(tz="Europe/Berlin")
        # drop timezone info, since Excel can not handle it
        # .dt.tz_localize(None)
    )

    df = df.set_index(["time"])
    # df.index = pd.to_datetime(df.index)
    # print(df)
    return df


def df_interpolate_missing_meter_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill gaps in meter data.

    -> 1min freq
    columns: 'meter' (Watt), kWh_total_in, kWh_total_out
    no missing rows
    """
    df = df.reset_index()

    # calc deltas to previous rows
    df["Delta_time"] = (df["time"] - df["time"].shift(1)).dt.total_seconds()  # in sec
    df["Delta_kWh_total_in"] = (df["kWh_total_in"] - df["kWh_total_in"].shift(1)) / df[
        "Delta_time"
    ]
    df["Delta_kWh_total_out"] = (
        df["kWh_total_out"] - df["kWh_total_out"].shift(1)
    ) / df["Delta_time"]
    # calc watt from deltas, used later for filling gaps
    df["watt_calc"] = (
        (df["Delta_kWh_total_in"] - df["Delta_kWh_total_out"]) * 1000 * 3600
    )

    # Convert from 10s freq to 1min freq and add missing times, using mean
    df = (
        df[["time", "watt", "watt_calc", "kWh_total_in", "kWh_total_out"]]
        .groupby([pd.Grouper(key="time", freq="1min")])
        .agg(
            {
                "watt": "mean",
                "watt_calc": "mean",
                "kWh_total_in": "last",
                "kWh_total_out": "last",
            }
        )
    )

    # print where it is NaN
    # print(df[df["watt"].isna()])

    # fill in gaps by backwards filling (bfill)
    df["watt_calc"] = df["watt_calc"].bfill()

    # overwrite watt by watt_calc where watt is missing
    df["watt"] = df["watt"].fillna(df["watt_calc"]).round(1)

    df = df.drop(columns=["watt_calc"])

    return df


def df_interpolate_missing_pv_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill gaps in pv data.

    -> 1min freq
    columns: "watt_last", "kWh_total"
    no missing rows
    """
    # df_pv calc delta
    df = df.reset_index()
    # calc deltas to previous rows
    df["Delta_time"] = (df["time"] - df["time"].shift(1)).dt.total_seconds()  # in sec
    df["Delta_kWh_total"] = (df["kWh_total"] - df["kWh_total"].shift(1)) / df[
        "Delta_time"
    ]

    # calc watt from deltas, used later for filling gaps
    df["watt_calc"] = df["Delta_kWh_total"] * 1000 * 3600
    # moving average over 9min
    # to prevent steps of 60W whenever the kWh increases by 0.001 (min accuracy)
    df["watt_calc"] = df["watt_calc"].rolling(window=9, min_periods=1).mean().round(3)

    # add missing times
    df = (
        df[["time", "watt_last", "watt_calc", "kWh_total"]]
        .groupby([pd.Grouper(key="time", freq="1min")])
        .agg({"watt_last": "mean", "watt_calc": "mean", "kWh_total": "last"})
    )

    # fill in gaps by backwards filling (bfill)
    df["watt_calc"] = df["watt_calc"].bfill()

    # overwrite df["watt"] with df["watt_calc"] where df["watt"].isna()
    df["watt_last"] = df["watt_last"].fillna(df["watt_calc"]).round(1)

    df = df.drop(columns=["watt_calc"])

    return df
