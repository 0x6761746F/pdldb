# pdldb

A high-performance analytical data store combining Polars' processing speed with Delta Lake's ACID transactions. This lightweight wrapper provides a database-like experience for local data processing.

## Overview

pdldb creates a columnar data store that offers:

- The speed and efficiency of Polars for data operations
- The reliability and versioning of Delta Lake for data integrity
- Simple database-like operations with table management
- Flexible storage options with both local and cloud-based implementations

## Installation

```python
pip install pdldb
```

## Example Table Life Cycle

```python
import polars as pl
import numpy as np
from datetime import datetime, timedelta
from pdldb import LocalLakeManager


# 1. Generate sample data
n_rows = int(10000)
np.random.seed(7)
start_date = datetime(2025, 3, 23)

data = {
    "sequence": np.arange(1, n_rows + 1, dtype=np.int32),
    "id": np.random.permutation(n_rows) + 1,
    "value_1": np.random.normal(100, 15, n_rows).astype(np.float32),
    "value_2": np.random.uniform(0, 1000, n_rows).astype(np.float32),
    "value_3": np.random.choice(["A", "B", "C", "D"], n_rows),
    "value_4": np.random.exponential(50, n_rows).astype(np.float32),
    "value_5": [(start_date + timedelta(seconds=i)) for i in range(n_rows)],
}

df = pl.DataFrame(data)

# Split data for demonstration purposes (overlapping rows)
df_part_one = df.slice(0, 6000)
df_part_two = df.slice(5000, 5000)

# 2. Initialize Delta Lake
lake = LocalLakeManager("pdldb_demo")

# 3. Define schema
schema = {
    "sequence": pl.Int32,
    "id": pl.Int64,
    "value_1": pl.Float32,
    "value_2": pl.Float32,
    "value_3": pl.Utf8,
    "value_4": pl.Float32,
    "value_5": pl.Datetime("ns"),
}

# 4. Create table and load initial data
lake.create_table("my_table", schema, primary_keys=["sequence", "value_5"])
lake.append_table("my_table", df_part_one)

# 5. Read data from table
initial_data = lake.get_data_frame("my_table")

# 6. Add new data (ignoring duplicate records)
lake.merge_table("my_table", df_part_two, merge_condition="insert")
merged_data = lake.get_data_frame("my_table")

# 7. Query full table with SQL and lazy evaluation
ldf = lake.get_lazy_frame("my_table")
result = ldf.sql("SELECT sequence, id FROM self WHERE sequence > 100").collect()

# 8. Get table metadata
tables = lake.list_tables()
table_info = lake.get_table_info("my_table")
schema = lake.get_table_schema("my_table")

# 9. Maintenance operations
lake.optimize_table("my_table")
lake.vacuum_table("my_table")

# 10. Overwrite table
small_df = df.slice(0, 50)
lake.overwrite_table("my_table", small_df)
after_overwrite = lake.get_data_frame("my_table")

# 11. Delete table
lake.delete_table("my_table")
```

More examples can be found in the example folder