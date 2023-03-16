import logging

from buoy_analytics.utils.buoy_model import to_BuoyModel
from buoy_analytics.utils.exceptions import NoDataRetrieved
from buoy_analytics.utils.ndbc_data import retrieve_buoy_data
from buoy_analytics.utils.store_data import to_sql_db


def main():
    logging.info("Retrieving active buoys")

    try:
        raw_buoys = retrieve_buoy_data()
    except NoDataRetrieved as err:
        logging.error(f"An error has occured: {err}")
        exit()

    logging.info("Active buoys retrieved")

    logging.info(raw_buoys[0])

    buoys = to_BuoyModel(raw_buoys)

    logging.info("Transformed data")

    for buoy in buoys:
        to_sql_db(buoy=buoy)
        break


if __name__ == "__main__":
    main()
