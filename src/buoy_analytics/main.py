import logging

from buoy_analytics.config import RAW_BUOY_DATA_URL
from buoy_analytics.utils.exceptions import NoDataRetrieved
from buoy_analytics.utils.ndbc_data import retrieve_buoy_data


def main():
    logging.info("Retrieving active buoys")

    try:
        raw_buoys = retrieve_buoy_data(url=RAW_BUOY_DATA_URL)
    except NoDataRetrieved as err:
        logging.error(f"An error has occured: {err}")
        exit()

    logging.info("Active buoys retrieved")
    print(raw_buoys)


if __name__ == "__main__":
    main()
