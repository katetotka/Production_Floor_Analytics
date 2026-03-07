%sql
CREATE OR REPLACE VIEW gold_dev.manufacturing.line_gr_np_47_sessions AS

SELECT
    start_timestamp,
    stop_timestamp,
    duration_seconds
FROM silver_dev.manufacturing.production_sessions
WHERE production_line_id = 'gr-np-47'
ORDER BY start_timestamp;