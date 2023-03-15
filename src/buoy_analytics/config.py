import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

RAW_BUOY_DATA_URL: str = "https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt"

DB_USER = ""
DB_PASSWORD = ""
DB_HOST = ""
DB_PORT = ""
DB_DATABASE = ""
