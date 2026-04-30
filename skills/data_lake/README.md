# data_lake

Central Athena connection and table catalog layer.

All scripts and notebooks that query `data_lake_curated_data` or `data_lake_raw_data`
should import from here — avoiding duplicate connection strings and hardcoded table names.

---

## Prerequisites

```bash
source scripts/refresh_aws_credentials.sh   # expires every 4 hours
source .venv/bin/activate
```

---

## Usage

### Simple reads (module-level functions)

```python
from skills.data_lake.scripts.connection import read_curated, read_raw

df = read_curated("SELECT * FROM sensor_samples LIMIT 10")
df = read_raw("SELECT * FROM unified_bee_frames LIMIT 10")
```

### Table name constants (no typos)

```python
from skills.data_lake.scripts.catalog import CURATED, RAW

df = read_curated(f"SELECT * FROM {CURATED.SENSOR_SAMPLES} LIMIT 5")
df = read_raw(f"SELECT * FROM {RAW.UNIFIED_BEE_FRAMES} LIMIT 5")
```

### Context manager (explicit session lifecycle)

```python
from skills.data_lake.scripts.connection import AthenaSession

with AthenaSession() as s:
    sensors  = s.read_curated("SELECT DISTINCT sensor_mac_address FROM sensor_samples LIMIT 100")
    gateways = s.read_curated("SELECT DISTINCT gateway_mac_address FROM gateway_samples LIMIT 100")
```

### Inside a ThreadPoolExecutor (thread-safe)

```python
from concurrent.futures import ThreadPoolExecutor
from skills.data_lake.scripts.connection import read_curated   # thread-local connections

def worker(sql):
    return read_curated(sql)   # each thread gets its own connection automatically

with ThreadPoolExecutor(max_workers=8) as pool:
    futures = [pool.submit(worker, sql) for sql in queries]
```

### Reader factory — pass a reader into a function

```python
from skills.data_lake.scripts.connection import make_reader, CURATED_DATABASE

read = make_reader(CURATED_DATABASE)
df   = read(f"SELECT * FROM {CURATED.SENSOR_SAMPLES} LIMIT 5")
```

### Live table discovery

```python
from skills.data_lake.scripts.catalog import list_tables, describe_table, sample, schema

list_tables()                                    # all tables in curated DB
list_tables("data_lake_raw_data")                # all tables in raw DB
describe_table("sensor_samples")                 # live column info from Athena
sample("gateway_samples", n=3)                   # first 3 rows
schema("sensor_samples")                         # local reference dict
```

---

## Skill layout

```
skills/data_lake/
  README.md
  spec/spec.txt              ← full table catalog, schemas, quirks, thread-safety notes
  scripts/
    connection.py            ← Athena connection factory
                               read_curated / read_raw / read
                               get_connection (thread-local)
                               make_reader (factory)
                               AthenaSession (context manager)
    catalog.py               ← CURATED.* / RAW.* table name constants
                               SCHEMA reference dict
                               list_tables / describe_table / sample / schema
  notebooks/                 ← ad-hoc exploration notebooks
```

---

## Known quirks

| Quirk | Rule |
|---|---|
| bee_frames table switch | Use `raw_bee_frames_table(date)` — never hardcode `supervised_beeframes` or `unified_bee_frames` |
| SQL `IN (x)` with 1 element | Some Athena engines reject single-element tuples — use `(x, x)` |
| pyathena thread safety | NOT thread-safe — use `get_connection()` or `make_reader()` inside workers |
| `pcb_temperature_one` encoding errors | Readings < −40°C → add 175.71 (handled in `temperature_data_export/transforms.py`) |
| gateway zero-acceleration | Drop rows where accel_x = accel_y = accel_z = 0 (bad hardware) |
