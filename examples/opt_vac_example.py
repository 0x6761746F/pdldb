import polars as pl
import os
import shutil
from pathlib import Path
from pdldb import LocalLakeManager
from examples.example_utils.synth_data import generate_synthetic_data
from examples.example_utils.stopwatch import stopwatch


def generate_test_data():
    print("Starting data generation or loading...")
    data_path = Path("examples/example_data/synthetic_data.parquet")
    if not data_path.exists():
        print("Data file not found - generating synthetic data...")
        generate_synthetic_data()
        df = pl.read_parquet(data_path)
        file_size_mb = data_path.stat().st_size / (1024 * 1024)
        print(f"Number of rows generated: {len(df):,}")
        print(f"Data file size: {file_size_mb:.2f} MB")
    else:
        print("Loading existing synthetic data from parquet file...")
        file_size_mb = data_path.stat().st_size / (1024 * 1024)
        df = pl.read_parquet(data_path)
        print(f"Number of rows loaded: {len(df):,}")
        print(f"Data file size: {file_size_mb:.2f} MB")
    return df


@stopwatch
def initialize_lake():
    print(
        "Initializing Delta Lake at 'examples/example_data/opt_vac_example/archives/delta_lake'..."
    )
    return LocalLakeManager("examples/example_data/opt_vac_example/archives/delta_lake")


@stopwatch
def create_table(lake, schema, table_name):
    print(f"Creating table '{table_name}' with defined schema...")
    print("Using primary keys: 'sequence' and 'value_5'")
    lake.create_table(table_name, schema, primary_keys=["sequence", "value_5"])
    print("Table created successfully")


@stopwatch
def write_batches(lake, df, table_name, batch_count=100):
    print(f"Writing data to '{table_name}' in {batch_count} batches...")

    batch_size = len(df) // batch_count
    total_rows_written = 0

    for i in range(batch_count):
        start_idx = i * batch_size
        batch_df = df.slice(start_idx, batch_size)
        total_rows_written += len(batch_df)

        print(f"  - Batch {i + 1}/{batch_count}: Writing {len(batch_df):,} rows")
        lake.append_table(table_name, batch_df)

    print(f"All batches complete. Total rows written: {total_rows_written:,}")


@stopwatch
def optimize_table(lake, table_name):
    print(f"Optimizing '{table_name}' to improve read performance...")
    print("This compacts small files and may create file statistics")
    lake.optimize_table(table_name)
    print("Optimization completed")


@stopwatch
def vacuum_table(lake, table_name):
    print(f"Vacuuming '{table_name}' to clean up old file versions...")
    print("This removes files no longer needed by the table")
    lake.vacuum_table(table_name)
    print("Vacuum operation completed")


def cleanup():
    print("Cleaning up test directories...")
    data_dir = "examples/example_data/opt_vac_example"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    print("Cleanup completed")


if __name__ == "__main__":
    print("=== DELTA LAKE OPTIMIZATION & VACUUM EXAMPLE ===")
    print(
        "This example demonstrates writing multiple batches followed by optimization and vacuum"
    )

    print("\n0. Cleanup old test data")
    cleanup()

    print("\n1. Data Preparation")
    df = generate_test_data()

    print("\n2. Lake Initialization")
    lake = initialize_lake()

    print("\n3. Schema Definition")
    schema = {
        "sequence": pl.Int32,
        "id": pl.Int64,
        "value_1": pl.Float32,
        "value_2": pl.Float32,
        "value_3": pl.Utf8,
        "value_4": pl.Float32,
        "value_5": pl.Datetime("ns"),
    }
    print(f"Defined schema with fields: {list(schema.keys())}")

    print("\n4. Table Creation")
    table_name = "my_table"
    create_table(lake, schema, table_name)

    print("\n5. Writing Multiple Batches")
    write_batches(lake, df, table_name, batch_count=100)

    print("\n6. Maintenance Operations")
    optimize_table(lake, table_name)
    vacuum_table(lake, table_name)

    print("\n=== EXAMPLE COMPLETED ===")
