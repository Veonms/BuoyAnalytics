import pandas as pd

from buoy_analytics.config import RAW_BUOY_DATA_URL
from buoy_analytics.utils.exceptions import NoDataRetrieved, retry


@retry(ExceptionToCheck=NoDataRetrieved)
def retrieve_buoy_data() -> list[dict]:
    """Retrieves raw buoy data from the NDBC.

    Raises:
        NoDataRetrieved: Request failed to return data.

    Returns:
        list[dict]: List of Dataframe columns with corresponding data.
    """

    df_active_buoys: pd.DataFrame = pd.read_csv(
        RAW_BUOY_DATA_URL,
        delim_whitespace=True,
    ).iloc[1:]

    if len(df_active_buoys.index) == 0:
        raise NoDataRetrieved("No data was retrieved.")

    return format_raw_data(df_active_buoys).to_dict("records")


def format_raw_data(df_buoys: pd.DataFrame) -> pd.DataFrame:
    """Create Timestamp and Location fields from retrieved data.
    Removes fields used to create Timestamp and Location.

    Args:
        df_buoys (pd.DataFrame): Dataframe containing raw buoy data.

    Returns:
        pd.DataFrame: Dataframe containing buoy data with Timestamp and Location fields.
        Fields LAT, LON, YYYY, MM, DD, hh and mm are removed.
    """

    df_buoys["Timestamp"] = (
        df_buoys[["YYYY", "MM", "DD"]].agg("-".join, axis=1)
        + " "
        + df_buoys[["hh", "mm"]].agg(":".join, axis=1)
        + ":00"
    )
    df_buoys["Location"] = df_buoys[["LAT", "LON"]].agg(",".join, axis=1)
    df_buoys.drop(["LAT", "LON", "YYYY", "MM", "DD", "hh", "mm"], axis=1, inplace=True)

    return df_buoys
