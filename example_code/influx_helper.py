"""InfluxDB helper functions."""

from influxdb import InfluxDBClient


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
