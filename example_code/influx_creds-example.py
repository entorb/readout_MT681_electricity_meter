"""Influx DB Credentials."""

# ruff: noqa
# cspell: disable

credentials_base = {
    "host": "raspi3",
    "port": 8086,
    "database": "raspi",
    "user": "uread",
    "password": "vffMV28KhBaBmfZTlkiz",
}

credentials_read = credentials_base.copy()
credentials_read["user"] = "ro_user"
credentials_read["password"] = "xxx"

credentials_write = credentials_base.copy()
credentials_write["user"] = "rw_user"
credentials_write["password"] = "yyy"
