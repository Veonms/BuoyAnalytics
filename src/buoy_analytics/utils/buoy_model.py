from pydantic import BaseModel


class BuoyModel(BaseModel):
    station: str
    timestamp: str
    location: str
    wind_direction: int | None = ...
    wind_speed: float | None = ...
    gust_speed: float | None = ...
    wave_height: float | None = ...
    dom_wave_period: float | None = ...
    avg_wave_period: float | None = ...
    dom_wave_direction: int | None = ...
    sea_pressure: float | None = ...
    air_temp: float | None = ...
    sea_surface_temp: float | None = ...
    dewpoint_temp: float | None = ...
    visability: float | None = ...
    pressure_tendency: float | None = ...
    water_level: float | None = ...


def to_BuoyModel(active_buoys: list[dict]) -> list[BuoyModel]:
    """Transforms data to BuoyModel object.
    Args:
        active_buoys (list[dict]): List of Dataframe columns with corresponing data.
    Returns:
        list[BuoyModel]: List of individual buoys.
    """

    buoys = [
        BuoyModel(
            station=str(buoy["#STN"]),
            timestamp=str(buoy["Timestamp"]),
            location=str(buoy["Location"]),
            wind_direction=None if buoy["WDIR"] == "MM" else int(buoy["WDIR"]),
            wind_speed=None if buoy["WSPD"] == "MM" else float(buoy["WSPD"]),
            gust_speed=None if buoy["GST"] == "MM" else float(buoy["GST"]),
            wave_height=None if buoy["WVHT"] == "MM" else float(buoy["WVHT"]),
            dom_wave_period=None if buoy["DPD"] == "MM" else float(buoy["DPD"]),
            avg_wave_period=None if buoy["APD"] == "MM" else float(buoy["APD"]),
            dom_wave_direction=None if buoy["MWD"] == "MM" else int(buoy["MWD"]),
            sea_pressure=None if buoy["PRES"] == "MM" else float(buoy["PRES"]),
            air_temp=None if buoy["ATMP"] == "MM" else float(buoy["ATMP"]),
            sea_surface_temp=None if buoy["WTMP"] == "MM" else float(buoy["WTMP"]),
            dewpoint_temp=None if buoy["DEWP"] == "MM" else float(buoy["DEWP"]),
            visability=None if buoy["VIS"] == "MM" else float(buoy["VIS"]),
            pressure_tendency=None if buoy["PTDY"] == "MM" else float(buoy["PTDY"]),
            water_level=None if buoy["TIDE"] == "MM" else float(buoy["TIDE"]),
        )
        for buoy in active_buoys
    ]

    return buoys
