"""
Fill missing watt data by interpolated values.
"""

from influx_creds import credentials_read, credentials_write
from influx_helper import (
    connect,
    connect_write_df,
    df_interpolate_missing_meter_data,
    # df_interpolate_missing_pv_data,
    fetch_data_to_pd,
    time_to_datetime,
)

client = connect(credentials_read)

# MT681 meter data
query = """
SELECT * FROM "tasmota_MT681"
WHERE time > now() - 1h
"""
df_meter = time_to_datetime(fetch_data_to_pd(client, query))

# # Shelly PV data
# query = """
# SELECT * FROM "Shelly3"
# WHERE room='Balkon'
# AND time > now() - 1d
# """
# df_pv = time_to_datetime(fetch_data_to_pd(client, query))

client.close()

df_meter = df_interpolate_missing_meter_data(df_meter)
# df_pv = df_interpolate_missing_pv_data(df_pv)

# filter on kWh_total_in is NA
df_meter = df_meter[df_meter["kWh_total_in"].isna()]
# df_pv = df_pv[df_pv["kWh_total"].isna()]

client = connect_write_df(credentials_write)
if len(df_meter) > 0:
    client.write_points(
        df_meter,
        "tasmota_MT681",
        protocol="line",
        batch_size=1000,
    )

# if len(df_pv) > 0:
#     client.write_points(
#         df_meter,
#         "Shelly3",
#         protocol="line",
#         batch_size=1000,
#     )

client.close()
