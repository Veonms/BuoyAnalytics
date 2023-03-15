from pytest import MonkeyPatch

from buoy_analytics.utils.store_data import (
    check_table_exists,
    query_database,
    retrieve_timestamp,
    store_buoy,
    to_sql_db,
)


def test_retrieve_timestamp_None(monkeypatch: MonkeyPatch):
    def return_none(*args, **kwargs):
        return [[None], ["Other data"]]

    monkeypatch.setattr("buoy_analytics.utils.store_data.query_database", return_none)
    assert retrieve_timestamp("") == 0


def test_retrieve_timestamp_Timestamp(monkeypatch: MonkeyPatch):
    def return_timestamp(*args, **kwargs):
        return [["2023-01-01"], ["Other data"]]

    monkeypatch.setattr(
        "buoy_analytics.utils.store_data.query_database", return_timestamp
    )
    assert retrieve_timestamp("") == "2023-01-01"
