from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from databricks.sdk.runtime import spark


def load_production_events(csv_path: str) -> DataFrame:
    """
    This function reads the raw CSV file and returns a dataframe with the production events.
    
    Expects columns: production_line_id, status, timestamp
    """
    df = (
        spark.read
        .option("header", "true")
        .option("sep", ",")
        .csv(csv_path)
    )

    df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

    return df