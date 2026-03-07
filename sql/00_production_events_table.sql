%sql
CREATE TABLE IF NOT EXISTS bronze_dev.manufacturing.production_events (
    PRODUCTION_LINE_ID STRING NOT NULL,
    STATUS STRING,
    EVENT_TIMESTAMP TIMESTAMP
);

-- In order to insert the data to the table, the dataset.csv was insert using pyspark in a variable name `pdf`.
-- Then, the variable was used to create a temporary view named `production_events_view`.
-- Then, the view was inserted to the table.

-- THE CODE IS SHOWN BELOW:

-- pdf = spark.read.option("header","true").option("sep",",").csv("file:/Workspace/Shared/Production_Floor_Analytics/data/dataset.csv")
-- pdf.createOrReplaceTempView("production_events_view")
-- spark.sql("INSERT INTO bronze_dev.manufacturing.production_events SELECT * FROM production_events_view")
