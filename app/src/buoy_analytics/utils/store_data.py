from datetime import MINYEAR, datetime

from buoy_analytics.config import DB_DATABASE, DB_HOST, DB_PASSWORD, DB_PORT, DB_USER
from buoy_analytics.utils.buoy_model import BuoyModel
from mysql import connector


def query_database(query: str):
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


def check_table_exists(station_id: str):
    """
    Creates a new table for the station if a table doesn't already exist.
    Makes one request to db rather than two (check if it exists and creates the table if not).
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
    query_database(query=query)
    return


def retrieve_timestamp(station_id: str) -> str:
    query = f"SELECT MAX(timestamp) FROM `{station_id}`;"
    max_timestamp = query_database(query=query)[0][0]

    if max_timestamp is None:
        return f"2000-01-01 00:00:00"
    return max_timestamp


def store_buoy(buoy: BuoyModel):
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

    query_database(query=query)
    return


def to_sql_db(buoy: BuoyModel):
    check_table_exists(buoy.station)
    max_db_timestamp = retrieve_timestamp(buoy.station)

    if datetime.strptime(buoy.timestamp, r"%Y-%m-%d %H:%M:%S") > datetime.strptime(
        max_db_timestamp, r"%Y-%m-%d %H:%M:%S"
    ):
        store_buoy(buoy=buoy)
