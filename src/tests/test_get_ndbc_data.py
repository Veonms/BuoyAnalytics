import pandas as pd
import pytest

from buoy_analytics.utils.exceptions import NoDataRetrieved
from buoy_analytics.utils.ndbc_data import format_raw_data, retrieve_buoy_data

"""Tests for function retrieve_buoy_data."""


def test_retrieve_buoy_data_NoDataRetrieved_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tests the case when no data is returned. This will result in an empty
    DataFrame, which should raise a NoDataRetrieved exception.
    The retry decorator will be called due to the NoDataRetrieved exception.
    """

    def empty_df(*args, **kwargs):
        return pd.DataFrame()

    monkeypatch.setattr("pandas.read_csv", empty_df)

    with pytest.raises(NoDataRetrieved):
        retrieve_buoy_data()


def test_retrieve_buoy_data_dict_returned(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests the case where data is retrieved from the API. The data from the API
    contains unwanted data in the first row, which is seen. Only the data part of
    the buoy data is used in this test.
    """

    def return_test_data(*args, **kwargs):
        test_data: dict = {
            "LAT": ["Unwanted data", "123"],
            "LON": ["Unwanted data", "456"],
            "YYYY": ["Unwanted data", "2023"],
            "MM": ["Unwanted data", "01"],
            "DD": ["Unwanted data", "01"],
            "hh": ["Unwanted data", "00"],
            "mm": ["Unwanted data", "00"],
        }
        return pd.DataFrame(data=test_data)

    monkeypatch.setattr("pandas.read_csv", return_test_data)

    assert retrieve_buoy_data() == [
        {
            "Timestamp": "2023-01-01 00:00:00",
            "Location": "123,456",
        }
    ]


"""Tests for function format_raw_data."""


def test_format_raw_data_formatted_data_returned() -> None:
    """Tests that the function returns the expected formatted data
    and drops the obsolete fields."""

    test_data: dict = {
        "LAT": ["123"],
        "LON": ["456"],
        "YYYY": ["2023"],
        "MM": ["01"],
        "DD": ["01"],
        "hh": ["00"],
        "mm": ["00"],
    }
    test_df = pd.DataFrame(data=test_data)

    formatted_data = {"Timestamp": ["2023-01-01 00:00:00"], "Location": ["123,456"]}

    formatted_df = pd.DataFrame(data=formatted_data)

    assert format_raw_data(df_buoys=test_df).equals(formatted_df)
