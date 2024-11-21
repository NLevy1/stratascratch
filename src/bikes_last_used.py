from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.functions import max as fmax

FINAL_COLUMN_ORDER = [
    "end_time",
    "bike_number",
]


def clean_input_df(input_df: DataFrame) -> DataFrame:
    """_summary_

    Args:
        input_df (DataFrame): _description_

    Returns:
        DataFrame: _description_
    """
    return input_df.withColumn("end_time", to_timestamp("end_time", "dd-M-yyyy H:mm"))


def groupby_max_end_time(input_df: DataFrame) -> DataFrame:
    """_summary_"""
    return input_df.groupby("bike_number").agg(fmax("end_time").alias("end_time")).sort(col("end_time").desc())


def get_bikes_last_used(input_df: DataFrame) -> DataFrame:
    """_summary_"""

    cleaned_input_df = clean_input_df(input_df)
    x = groupby_max_end_time(cleaned_input_df)
    return x.select(FINAL_COLUMN_ORDER)
