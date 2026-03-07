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

## PySpark Implementation

The same logic was rewritten as a Python package inside the `python/` folder. Each file mirrors a layer from the SQL implementation:

| File | Layer | What it does |
|------|-------|-------------|
| `bronze.py` | Bronze | `load_production_events()` reads the CSV and returns a DataFrame |
| `silver.py` | Silver | `build_production_sessions()` pairs START/STOP into sessions |
| `gold.py` | Gold | `get_line_sessions()`, `get_floor_uptime_downtime()`, `get_most_downtime_line()` |
| `__init__.py` | | Exports all functions so they can be imported directly |

The `run_analysis` notebook sits outside the `python/` folder and runs the full pipeline:

```python
import sys
sys.path.append("/Workspace/Shared/Production_Floor_Analytics")

from python import load_production_events, build_production_sessions
from python import get_line_sessions, get_floor_uptime_downtime, get_most_downtime_line

csv_path = "file:/Workspace/Shared/Production_Floor_Analytics/data/dataset.csv"
events = load_production_events(csv_path)
events.show(5)

sessions = build_production_sessions(events)
sessions.show()

get_line_sessions(sessions, "gr-np-47").show()
get_floor_uptime_downtime(sessions).show()
get_most_downtime_line(sessions).show()
```

## How to Run

### Prerequisites

- Databricks workspace
- A cluster with PySpark
- The `dataset.csv` file placed inside the `data/` folder, within the project root folder `Production_Floor_Analytics`, which should be created under the `Shared` Workspace.

### Running the SQL Implementation

1. Open each SQL script in a Databricks SQL notebook.
2. Run them in order, starting from `00` to `04`. **NOTE:** Before running `01`, the data need to be inserted to the `original bronze table`. In order to do that, a notebook needs to be created first that runs the commented code in .sql `00`.
3. The bronze table and silver table will be created first.
4. Then the gold views will be available for querying.

```sql
-- After running all scripts, the gold views can be queried directly
SELECT * FROM gold_dev.manufacturing.line_gr_np_47_sessions;
SELECT * FROM gold_dev.manufacturing.floor_uptime_downtime;
SELECT * FROM gold_dev.manufacturing.most_downtime_line;
```

> **Note:** In Databricks, tables are referenced using a three-part naming convention: `<catalog>.<schema>.<table_name>`. The catalogs (`bronze_dev`, `silver_dev`, `gold_dev`) and schemas (`manufacturing`) used in this project  are specific to my workspace. Replace them with your own catalogs and schemas as needed.
