%sql
CREATE OR REPLACE VIEW gold_dev.manufacturing.floor_uptime_downtime AS

WITH per_line AS (
    SELECT
        production_line_id,
        SUM(duration_seconds) AS uptime_seconds,
        (
            CAST(MAX(stop_timestamp) AS LONG)
            - CAST(MIN(start_timestamp) AS LONG)
        ) - SUM(duration_seconds) AS downtime_seconds
    FROM silver_dev.manufacturing.production_sessions
    GROUP BY production_line_id
)

SELECT
    SUM(uptime_seconds)   AS total_uptime_seconds,
    SUM(downtime_seconds)  AS total_downtime_seconds
FROM per_line;