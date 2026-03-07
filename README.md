# Production Floor Analytics Mini Project 
Production line monitoring using SQL and PySpark on Databricks. Raw START/ON/STOP events flow through a medallion architecture. Bronze table ingests, silver table builds sessions, gold views deliver KPIs. Calculation of uptime, downtime, and flags the worst-performing lines (KPIs) using also Python functions. Below, the flow is depicted regarding `SQL` and `PySpark` implementation:

## SQL Implementation

The SQL scripts are numbered according to their execution order:

| Script | Layer | Description |
|--------|-------|-------------|
| `00_production_events_table.sql` | Bronze | Creates the raw events table and loads the CSV data |
| `01_silver_production_sessions.sql` | Silver | Pairs START/STOP events into production sessions |
| `02_gold_line_gr_np_47_sessions.sql` | Gold | Sessions for a specific line (gr-np-47) |
| `03_gold_floor_uptime_downtime.sql` | Gold | Total uptime vs downtime across the floor |
| `04_gold_most_downtime_line.sql` | Gold | Identifies the line with the most downtime |

### Medallion Architecture

Medallion architecture was used (bronze → silver → gold) because it keeps the analytical process more organized:

- **Bronze layer** holds the raw data, as-is from the CSV. No transformations, just ingestion.
- **Silver layer** is where the actual logic lives — pairing each START with its corresponding STOP to form sessions.
- **Gold layer** is where the business questions get answered.

### Why Tables were used for Bronze and Silver, but Views for Gold?

Bronze layer holds a table, because raw data needs to be persisted. This is where raw data are ingested from the corresponding sources. It is the foundation of everything downstream. If something goes wrong in silver or gold, we can always reprocess from bronze without needing the original file again.

Silver layer is also a table because every gold query depends on it. If silver was a view, every time a gold view was queried, it would recompute the session pairing logic all over again. A table was selected because that work is done once and stored, making all downstream queries faster.

Gold layer uses views because they are just lightweight aggregations on top of silver. They don't need to store anything, they just reshape what's already there. And when the silver table gets refreshed with new data, the gold views automatically reflect the updated results.
