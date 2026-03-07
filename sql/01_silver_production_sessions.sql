-- Step 1: Create the table
CREATE TABLE IF NOT EXISTS silver_dev.manufacturing.production_sessions (
    PRODUCTION_LINE_ID STRING,
    START_TIMESTAMP TIMESTAMP,
    STOP_TIMESTAMP TIMESTAMP,
    DURATION_SECONDS LONG
);

-- Step 2: Insert the data
INSERT INTO silver_dev.manufacturing.production_sessions

WITH start_events AS (
    SELECT
        production_line_id,
        event_timestamp AS start_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY production_line_id
            ORDER BY event_timestamp
        ) AS session_seq
    FROM bronze_dev.manufacturing.production_events
    WHERE status = 'START'
),

stop_events AS (
    SELECT
        production_line_id,
        event_timestamp AS stop_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY production_line_id
            ORDER BY event_timestamp
        ) AS session_seq
    FROM bronze_dev.manufacturing.production_events
    WHERE status = 'STOP'
)

SELECT
    s.production_line_id,
    s.start_timestamp,
    e.stop_timestamp,
    CAST(e.stop_timestamp AS LONG) - CAST(s.start_timestamp AS LONG) AS duration_seconds
FROM start_events s
INNER JOIN stop_events e
    ON s.production_line_id = e.production_line_id
    AND s.session_seq = e.session_seq;