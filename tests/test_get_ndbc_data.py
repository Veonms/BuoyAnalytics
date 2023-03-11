import pandas as pd
import pytest

from buoy_analytics.utils.exceptions import NoDataRetrieved
from buoy_analytics.utils.ndbc_data import format_raw_data, retrieve_buoy_data


def test_retrieve_buoy_data_NoDataRetrieved_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tests the case when no data is returned. This will result in an empty
    DataFrame, which should raise a NoDataRetrieved exception.
    The retry decorator will be called due to the NoDataRetrieved exception.

    Args:
        monkeypatch (pytest.MonkeyPatch)
    """

    def empty_df(*args, **kwargs):
        return pd.DataFrame()

    monkeypatch.setattr("pandas.read_csv", empty_df)

    with pytest.raises(NoDataRetrieved):
        retrieve_buoy_data()


def test_format_raw_data_formatted_data_returned() -> None:
    """Tests that the function returns the expected formatted data
    and drops the obsolete fields."""

    test_data = {
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
