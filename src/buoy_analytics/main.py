import logging

from buoy_analytics.utils.buoy_model import to_BuoyModel
from buoy_analytics.utils.exceptions import NoDataRetrieved
from buoy_analytics.utils.ndbc_data import retrieve_buoy_data


def main():
    logging.info("Retrieving active buoys")

    try:
        raw_buoys = retrieve_buoy_data()
    except NoDataRetrieved as err:
        logging.error(f"An error has occured: {err}")
        exit()

    logging.info("Active buoys retrieved")

    buoys = to_BuoyModel(raw_buoys)

    logging.info("Transformed data")


if __name__ == "__main__":
    main()
