%sql
CREATE OR REPLACE VIEW gold_dev.manufacturing.most_downtime_line AS

WITH per_line AS (
    SELECT
        production_line_id,
        (
            CAST(MAX(stop_timestamp) AS LONG)
            - CAST(MIN(start_timestamp) AS LONG)
        ) - SUM(duration_seconds) AS downtime_seconds
    FROM silver_dev.manufacturing.production_sessions
    GROUP BY production_line_id
)

SELECT
    production_line_id,
    downtime_seconds
FROM per_line
ORDER BY downtime_seconds DESC
LIMIT 1;