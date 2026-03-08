from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, unix_timestamp
from pyspark.sql.window import Window
from databricks.sdk.runtime import spark


def build_production_sessions(events_df: DataFrame) -> DataFrame:
    """
    This function takes the raw events (START/STOP) and pairs them into sessions.
    
    Each session has: production_line_id, start_timestamp, stop_timestamp, duration_seconds.
    
    For each line, we match the 1st START with the 1st STOP, the second START with the second STOP, etc.
    """

    line_window = Window.partitionBy("production_line_id").orderBy("timestamp")

    starts = (
        events_df
        .filter(col("status") == "START")
        .withColumn("session_seq", row_number().over(line_window))
        .select(
            col("production_line_id"),
            col("timestamp").alias("start_timestamp"),
            col("session_seq"),
        )
    )

    stops = (
        events_df
        .filter(col("status") == "STOP")
        .withColumn("session_seq", row_number().over(line_window))
        .select(
            col("production_line_id"),
            col("timestamp").alias("stop_timestamp"),
            col("session_seq"),
        )
    )

    sessions = (
        starts
        .join(stops, on=["production_line_id", "session_seq"], how="inner")
        .withColumn(
            "duration_seconds",
            unix_timestamp("stop_timestamp") - unix_timestamp("start_timestamp"),
        )
        .drop("session_seq")
    )

    return sessions