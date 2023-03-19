import logging
from datetime import datetime

from mysql import connector
from mysql.connector.errors import DatabaseError

from buoy_analytics.config import DB_DATABASE, DB_HOST, DB_PASSWORD, DB_PORT, DB_USER
from buoy_analytics.utils.buoy_model import BuoyModel
from buoy_analytics.utils.exceptions import (
    NoDataRetrieved,
    SQLQueryExecutionFailed,
    retry,
)


@retry(ExceptionToCheck=DatabaseError)
def query_database(query: str):
    """Takes in a query as a parameter and will execute the query against
    the database defined in the config. Returns the result from the query.

    Args:
        query (str): Query to execute.

    Returns:
        _type_: Response from the query.
    """
    with connector.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_DATABASE,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            res = cursor.fetchall()

        conn.commit()
        return res


def check_table_exists(station_id: str) -> None:
    """Creates a new table for the station if a table doesn't already exist.
    Makes one request to db rather than two (check if it exists and creates the table if not).

    Args:
        station_id (str): Station ID for the specific buoy.
    """

    query = f"""
        CREATE TABLE IF NOT EXISTS `{station_id}` (
            station varchar(255),
            timestamp varchar(255),
            location varchar(255),
            wind_direction int,
            wind_speed float(53),
            gust_speed float(53),
            wave_height float(53),
            dom_wave_period float(53),
            avg_wave_period float(53),
            dom_wave_direction int,
            sea_pressure float(53),
            air_temp float(53),
            sea_surface_temp float(53),
            dewpoint_temp float(53),
            visability float(53),
            pressure_tendency float(53),
            water_level float(53)
        );
        """
    try:
        query_database(query=query)

    except DatabaseError as err:
        logging.error(f"Could not check if table exists for station {station_id}.")
        raise err

    return


def retrieve_timestamp(station_id: str) -> str:
    """Takes in a station id and generates a query using the id. Uses the
    query_database function to execute the query, where the result is retrieved.
    If no data exists, then None will be returned by the query, where it will be
    replaced by 2000-01-01 00:00:00 to allow comparisons.

    Args:
        station_id (str): Station ID for the specific buoy.

    Returns:
        str: Returns the max timestamp contained in the database. If no data is
        contained in the database, 2000-01-01 00:00:00 is returned.
    """
    query = f"SELECT MAX(timestamp) FROM `{station_id}`;"

    try:
        max_timestamp = query_database(query=query)[0][0]

    except DatabaseError as err:
        logging.error(f"Could not get timestamp for station {station_id}.")
        raise err

    if max_timestamp is None:
        return f"2000-01-01 00:00:00"
    return max_timestamp


def store_buoy(buoy: BuoyModel) -> None:
    """Creates a new table for the station if a table doesn't already exist.
    Makes one request to db rather than two (check if it exists and creates the table if not).

    Args:
        buoy (BuoyModel): Individual buoy.
    """
    query = f"""
    INSERT INTO `{buoy.station}` (
        station, 
        timestamp, 
        location, 
        wind_direction, 
        wind_speed, 
        gust_speed, 
        wave_height, 
        dom_wave_period, 
        avg_wave_period, 
        dom_wave_direction, 
        sea_pressure, 
        air_temp, 
        sea_surface_temp, 
        dewpoint_temp, 
        visability, 
        pressure_tendency, 
        water_level
    )
    VALUES (
        {'NULL' if buoy.station is None else f"'{buoy.station}'"},
        {'NULL' if buoy.timestamp is None else f"'{buoy.timestamp}'"},
        {'NULL' if buoy.location is None else f"'{buoy.location}'"},
        {'NULL' if buoy.wind_direction is None else f"'{buoy.wind_direction}'"},
        {'NULL' if buoy.wind_speed is None else f"'{buoy.wind_speed}'"},
        {'NULL' if buoy.gust_speed is None else f"'{buoy.gust_speed}'"},
        {'NULL' if buoy.wave_height is None else f"'{buoy.wave_height}'"},
        {'NULL' if buoy.dom_wave_period is None else f"'{buoy.dom_wave_period}'"},
        {'NULL' if buoy.avg_wave_period is None else f"'{buoy.avg_wave_period}'"},
        {'NULL' if buoy.dom_wave_direction is None else f"'{buoy.dom_wave_direction}'"},
        {'NULL' if buoy.sea_pressure is None else f"'{buoy.sea_pressure}'"},
        {'NULL' if buoy.air_temp is None else f"'{buoy.air_temp}'"},
        {'NULL' if buoy.sea_surface_temp is None else f"'{buoy.sea_surface_temp}'"},
        {'NULL' if buoy.dewpoint_temp is None else f"'{buoy.dewpoint_temp}'"},
        {'NULL' if buoy.visability is None else f"'{buoy.visability}'"},
        {'NULL' if buoy.pressure_tendency is None else f"'{buoy.pressure_tendency}'"},
        {'NULL' if buoy.water_level is None else f"'{buoy.water_level}'"}
    );
    """

    try:
        query_database(query=query)

    except DatabaseError as err:
        logging.error(f"Could not buoy data for station {buoy.station}.")
        raise err

    return


def to_sql_db(buoy: BuoyModel):

    try:
        check_table_exists(buoy.station)
    except DatabaseError as err:
        logging.error(err)
        raise SQLQueryExecutionFailed("SQL query could not be executed.")

    try:
        max_db_timestamp = retrieve_timestamp(buoy.station)
    except DatabaseError as err:
        logging.error(err)
        raise NoDataRetrieved(
            f"Timestamp could not be retrieved for station {buoy.station}."
        )

    if not datetime.strptime(buoy.timestamp, r"%Y-%m-%d %H:%M:%S") > datetime.strptime(
        max_db_timestamp, r"%Y-%m-%d %H:%M:%S"
    ):
        return

    try:
        store_buoy(buoy=buoy)
    except DatabaseError as err:
        logging.error(err)
        raise SQLQueryExecutionFailed("SQL query could not be executed.")

    return
