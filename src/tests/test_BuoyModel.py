from buoy_analytics.utils.buoy_model import BuoyModel, to_BuoyModel


def test_to_BuoyModel_None_fields() -> None:
    """Tests case where all possible fields have None values."""
    test_buoy_dict = [
        {
            "#STN": "13009",
            "Timestamp": "2023-03-11 22:00:00",
            "Location": "8.000,-38.000",
            "WDIR": "MM",
            "WSPD": "MM",
            "GST": "MM",
            "WVHT": "MM",
            "DPD": "MM",
            "APD": "MM",
            "MWD": "MM",
            "PRES": "MM",
            "PTDY": "MM",
            "ATMP": "MM",
            "WTMP": "MM",
            "DEWP": "MM",
            "VIS": "MM",
            "TIDE": "MM",
        }
    ]

    transformed_BuoyModel = to_BuoyModel(test_buoy_dict)

    transformed_test_BuoyModel = BuoyModel(
        station="13009",
        timestamp="2023-03-11 22:00:00",
        location="8.000,-38.000",
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
        visibility=None,
        pressure_tendency=None,
        water_level=None,
    )

    assert transformed_BuoyModel[0].__dict__ == transformed_test_BuoyModel.__dict__
