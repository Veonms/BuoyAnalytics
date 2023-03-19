import pytest
from mysql.connector.errors import DatabaseError
from pytest import MonkeyPatch

from buoy_analytics.utils.buoy_model import BuoyModel
from buoy_analytics.utils.exceptions import NoDataRetrieved, SQLQueryExecutionFailed
from buoy_analytics.utils.store_data import (
    check_table_exists,
    retrieve_timestamp,
    store_buoy,
    to_sql_db,
)

"""Tests for function check_table_exists"""


def test_check_table_exists_returns_none(monkeypatch: MonkeyPatch):
    """Test to check that the function returns when the query doesn't raise an exception."""

    def immediate_return(*args, **kwargs):
        return

    monkeypatch.setattr(
        "buoy_analytics.utils.store_data.query_database", immediate_return
    )
    assert check_table_exists("") is None


def test_check_table_exists_raises_DatabaseError(monkeypatch: MonkeyPatch):
    """Test to check that the function will re-raise the DatabaseError exception."""

    def return_DatabaseError(*args, **kwargs):
        raise DatabaseError

    monkeypatch.setattr(
        "buoy_analytics.utils.store_data.query_database", return_DatabaseError
    )

    with pytest.raises(DatabaseError):
        check_table_exists("")


"""Tests for function retrieve_timestamp"""


def test_retrieve_timestamp_None(monkeypatch: MonkeyPatch):
    """Test to check that the function returns when the query doesn't raise an exception"""

    def return_none(*args, **kwargs):
        return [[None], ["Other data"]]

    monkeypatch.setattr("buoy_analytics.utils.store_data.query_database", return_none)
    assert retrieve_timestamp("") == "2000-01-01 00:00:00"


def test_retrieve_timestamp_valid_timestamp(monkeypatch: MonkeyPatch):
    """Test to check that the function returns the timestamp retrieved from the query."""

    def return_timestamp(*args, **kwargs):
        return [["2023-01-01"], ["Other data"]]

    monkeypatch.setattr(
        "buoy_analytics.utils.store_data.query_database", return_timestamp
    )
    assert retrieve_timestamp("") == "2023-01-01"


def test_retrieve_timestamp_raises_DatabaseError(monkeypatch: MonkeyPatch):
    """Test to check that the function re-raises the DatabaseError exception."""

    def return_DatabaseError(*args, **kwargs):
        raise DatabaseError

    monkeypatch.setattr(
        "buoy_analytics.utils.store_data.query_database", return_DatabaseError
    )

    with pytest.raises(DatabaseError):
        retrieve_timestamp("")


"""Tests for function store_buoy"""


def test_store_buoy_returns_none(monkeypatch: MonkeyPatch):
    """Test to check that the function returns when the query doesn't raise an exception."""

    def immediate_return(*args, **kwargs):
        return

    monkeypatch.setattr(
        "buoy_analytics.utils.store_data.query_database", immediate_return
    )

    test_buoy = BuoyModel(
        station="test_station_id",
        timestamp="test_timestamp",
        location="test_location",
        wind_direction=None,
        wind_speed=None,
        gust_speed=None,
        wave_height=None,
        dom_wave_period=None,
        avg_wave_period=None,
        dom_wave_direction=None,
        sea_pressure=None,
        air_temp=None,
        sea_surface_temp=None,
        dewpoint_temp=None,
        visability=None,
        pressure_tendency=None,
        water_level=None,
    )

    assert store_buoy(buoy=test_buoy) is None


def test_store_buoy_raises_DatabaseError(monkeypatch: MonkeyPatch):
    """Test to check that the function re-raises the DatabaseError exception."""

    def return_DatabaseError(*args, **kwargs):
        raise DatabaseError

    monkeypatch.setattr(
        "buoy_analytics.utils.store_data.query_database", return_DatabaseError
    )

    test_buoy = BuoyModel(
        station="test_station_id",
        timestamp="test_timestamp",
        location="test_location",
        wind_direction=None,
        wind_speed=None,
        gust_speed=None,
        wave_height=None,
        dom_wave_period=None,
        avg_wave_period=None,
        dom_wave_direction=None,
        sea_pressure=None,
        air_temp=None,
        sea_surface_temp=None,
        dewpoint_temp=None,
        visability=None,
        pressure_tendency=None,
        water_level=None,
    )

    with pytest.raises(DatabaseError):
        store_buoy(test_buoy)


"""Tests for function to_sql_db"""


def test_to_sql_db_returns_none(monkeypatch: MonkeyPatch):
    """Test to check that the function returns when the queries don't raise an exception."""

    def immediate_return(*args, **kwargs):
        return

    def return_timestamp(*args, **kwargs):
        return "2020-01-01 00:00:00"

    monkeypatch.setattr(
        "buoy_analytics.utils.store_data.check_table_exists", immediate_return
    )

    monkeypatch.setattr(
        "buoy_analytics.utils.store_data.retrieve_timestamp", return_timestamp
    )

    monkeypatch.setattr("buoy_analytics.utils.store_data.store_buoy", immediate_return)

    test_buoy = BuoyModel(
        station="test_station_id",
        timestamp="2023-01-01 00:00:00",
        location="test_location",
        wind_direction=None,
        wind_speed=None,
        gust_speed=None,
        wave_height=None,
        dom_wave_period=None,
        avg_wave_period=None,
        dom_wave_direction=None,
        sea_pressure=None,
        air_temp=None,
        sea_surface_temp=None,
        dewpoint_temp=None,
        visability=None,
        pressure_tendency=None,
        water_level=None,
    )

    assert to_sql_db(buoy=test_buoy) is None


def test_to_sql_db_timestamp_not_new(monkeypatch: MonkeyPatch):
    """Test to check that the function returns when the timestamp retrieved is not new."""

    def immediate_return(*args, **kwargs):
        return

    def return_timestamp(*args, **kwargs):
        return "2023-01-01 00:00:00"

    monkeypatch.setattr(
        "buoy_analytics.utils.store_data.check_table_exists", immediate_return
    )

    monkeypatch.setattr(
        "buoy_analytics.utils.store_data.retrieve_timestamp", return_timestamp
    )

    test_buoy = BuoyModel(
        station="test_station_id",
        timestamp="2020-01-01 00:00:00",
        location="test_location",
        wind_direction=None,
        wind_speed=None,
        gust_speed=None,
        wave_height=None,
        dom_wave_period=None,
        avg_wave_period=None,
        dom_wave_direction=None,
        sea_pressure=None,
        air_temp=None,
        sea_surface_temp=None,
        dewpoint_temp=None,
        visability=None,
        pressure_tendency=None,
        water_level=None,
    )

    assert to_sql_db(buoy=test_buoy) is None


def test_to_sql_db_table_check_raises_DatabaseError(monkeypatch: MonkeyPatch):
    """Test to check that the function raises a SQLQueryExecutionFailed exception
    when check_table_exists raises a DatabaseError exception."""

    def return_DatabaseError(*args, **kwargs):
        raise DatabaseError

    monkeypatch.setattr(
        "buoy_analytics.utils.store_data.check_table_exists", return_DatabaseError
    )

    test_buoy = BuoyModel(
        station="test_station_id",
        timestamp="test_timestamp",
        location="test_location",
        wind_direction=None,
        wind_speed=None,
        gust_speed=None,
        wave_height=None,
        dom_wave_period=None,
        avg_wave_period=None,
        dom_wave_direction=None,
        sea_pressure=None,
        air_temp=None,
        sea_surface_temp=None,
        dewpoint_temp=None,
        visability=None,
        pressure_tendency=None,
        water_level=None,
    )

    with pytest.raises(SQLQueryExecutionFailed):
        to_sql_db(buoy=test_buoy)


def test_to_sql_db_retrieve_timestamp_raises_DatabaseError(monkeypatch: MonkeyPatch):
    """Test to check that the function raises a NoDataRetrieved exception
    when retrieve_timestamp raises a DatabaseError exception."""

    def return_DatabaseError(*args, **kwargs):
        raise DatabaseError

    def immediate_return(*args, **kwargs):
        return

    monkeypatch.setattr(
        "buoy_analytics.utils.store_data.check_table_exists", immediate_return
    )

    monkeypatch.setattr(
        "buoy_analytics.utils.store_data.retrieve_timestamp", return_DatabaseError
    )

    test_buoy = BuoyModel(
        station="test_station_id",
        timestamp="2023-01-01 00:00:00",
        location="test_location",
        wind_direction=None,
        wind_speed=None,
        gust_speed=None,
        wave_height=None,
        dom_wave_period=None,
        avg_wave_period=None,
        dom_wave_direction=None,
        sea_pressure=None,
        air_temp=None,
        sea_surface_temp=None,
        dewpoint_temp=None,
        visability=None,
        pressure_tendency=None,
        water_level=None,
    )

    with pytest.raises(NoDataRetrieved):
        to_sql_db(buoy=test_buoy)


def test_to_sql_db_store_buoy_raises_DatabaseError(monkeypatch: MonkeyPatch):
    """Test to check that the function raises a SQLQueryExecutionFailed exception
    when store_buoy raises a DatabaseError exception."""

    def return_DatabaseError(*args, **kwargs):
        raise DatabaseError

    def immediate_return(*args, **kwargs):
        return

    def return_timestamp(*args, **kwargs):
        return "2020-01-01 00:00:00"

    monkeypatch.setattr(
        "buoy_analytics.utils.store_data.check_table_exists", immediate_return
    )

    monkeypatch.setattr(
        "buoy_analytics.utils.store_data.retrieve_timestamp", return_timestamp
    )

    monkeypatch.setattr(
        "buoy_analytics.utils.store_data.store_buoy", return_DatabaseError
    )

    test_buoy = BuoyModel(
        station="test_station_id",
        timestamp="2023-01-01 00:00:00",
        location="test_location",
        wind_direction=None,
        wind_speed=None,
        gust_speed=None,
        wave_height=None,
        dom_wave_period=None,
        avg_wave_period=None,
        dom_wave_direction=None,
        sea_pressure=None,
        air_temp=None,
        sea_surface_temp=None,
        dewpoint_temp=None,
        visability=None,
        pressure_tendency=None,
        water_level=None,
    )

    with pytest.raises(SQLQueryExecutionFailed):
        to_sql_db(buoy=test_buoy)
