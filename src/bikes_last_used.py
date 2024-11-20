from pyspark.sql import DataFrame


def run_script(input_df: DataFrame):
    """_summary_"""

    columns_to_select_bikes = [
        "duration",
        "duration_seconds",
        "start_time",
        "start_station",
        "start_terminal",
        "end_time",
        "end_station",
        "end_terminal",
        "bike_number",
        "rider_type",
        "id",
    ]

    return input_df.select(columns_to_select_bikes)
