import polars as pl
import numpy as np
import os
from datetime import datetime, timedelta


def generate_synthetic_data(
    target_size_mb=2048,
    seed=42,
    output_file="examples/example_data/synthetic_data.parquet",
):
    bytes_per_row = 44
    target_size_bytes = int(target_size_mb * 1024**2)
    n_rows = int(target_size_bytes / bytes_per_row)
    np.random.seed(seed)

    start_date = datetime(2023, 1, 1)

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

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    df.write_parquet(
        output_file,
        compression="snappy",
    )
