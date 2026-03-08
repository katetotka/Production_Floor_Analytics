from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as _sum, max as _max, min as _min, unix_timestamp
from databricks.sdk.runtime import spark


def get_line_sessions(sessions_df: DataFrame, line_id: str) -> DataFrame:
    """
    This function returns all sessions for a specific production line, ordered by start time.
    
    Example: get_line_sessions(sessions, "gr-np-47")
    """
    return (
        sessions_df
        .filter(col("production_line_id") == line_id)
        .orderBy("start_timestamp")
    )


def get_floor_uptime_downtime(sessions_df: DataFrame) -> DataFrame:
    """
    This function calculates total uptime and downtime across the entire production floor.
    
    Downtime per line = (last stop - first start) - total uptime for that line.
    After that, all are summed up.
    
    Returns one row: total_uptime_seconds, total_downtime_seconds
    """
    per_line = (
        sessions_df
        .groupBy("production_line_id")
        .agg(
            _sum("duration_seconds").alias("uptime_seconds"),
            (
                _max(unix_timestamp("stop_timestamp"))
                - _min(unix_timestamp("start_timestamp"))
            ).alias("total_span_seconds"),
        )
        .withColumn("downtime_seconds", col("total_span_seconds") - col("uptime_seconds"))
    )

    result = per_line.agg(
        _sum("uptime_seconds").alias("total_uptime_seconds"),
        _sum("downtime_seconds").alias("total_downtime_seconds"),
    )

    return result


def get_most_downtime_line(sessions_df: DataFrame) -> DataFrame:
    """
    This function finds the production line with the most downtime.
    Returns one row: production_line_id, downtime_seconds.
    """
    per_line = (
        sessions_df
        .groupBy("production_line_id")
        .agg(
            _sum("duration_seconds").alias("uptime_seconds"),
            (
                _max(unix_timestamp("stop_timestamp"))
                - _min(unix_timestamp("start_timestamp"))
            ).alias("total_span_seconds"),
        )
        .withColumn("downtime_seconds", col("total_span_seconds") - col("uptime_seconds"))
        .select("production_line_id", "downtime_seconds")
        .orderBy(col("downtime_seconds").desc())
        .limit(1)
    )

    return per_line