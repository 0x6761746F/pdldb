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


def split_data_with_overlap(df, overlap_percentage=0.2):
    print(f"Splitting data with {overlap_percentage * 100}% overlap...")

    total_rows = len(df)
    halfway = total_rows // 2
    overlap_size = int(total_rows * overlap_percentage)
    second_start_idx = halfway - overlap_size
    first_half = df.slice(0, halfway + overlap_size)
    second_half = df.slice(second_start_idx, total_rows - second_start_idx)
    overlap_count = overlap_size * 2

    print("Split complete:")
    print(f"  - First half: {len(first_half):,} rows")
    print(f"  - Second half: {len(second_half):,} rows")
    print(f"  - Overlapping rows: {overlap_count:,}")

    return first_half, second_half, overlap_count


@stopwatch
def initialize_lake():
    print(
        "Initializing Delta Lake at 'examples/example_data/full_example/archives/delta_lake'..."
    )
    return LocalLakeManager("examples/example_data/full_example/archives/delta_lake")


@stopwatch
def create_table(lake, schema):
    print("Creating table 'my_table' with defined schema...")
    print("Using primary keys: 'sequence' and 'value_5'")
    lake.create_table("my_table", schema, primary_keys=["sequence", "value_5"])
    print("Table created successfully")


@stopwatch
def write_table(lake, df):
    print(f"Writing {len(df):,} rows to 'my_table'...")
    lake.append_table("my_table", df)
    print("Write operation completed")


@stopwatch
def read_table(lake):
    print("Reading all data from 'my_table'...")
    result = lake.get_data_frame("my_table")
    print("Data preview:")
    print(result)
    return result


@stopwatch
def merge_table(lake, df):
    print(f"Merging {len(df):,} rows into 'my_table'...")
    print(
        "This will insert only new data based on the primary keys (sequence, value_5)"
    )
    lake.merge_table("my_table", df)
    print("Merge operation completed - No duplicates should be present")


@stopwatch
def read_table_post_merge(lake):
    print("Reading all data from 'my_table'...")
    result = lake.get_data_frame("my_table")
    print("Data preview:")
    print(result)
    return result


@stopwatch
def read_table_lazy(lake):
    print("Reading all data from 'my_table' lazily via SQL query...")
    ldf = lake.get_lazy_frame("my_table")
    ldf = ldf.sql("SELECT * FROM self")
    result = ldf.collect()
    print(result)
    return result


@stopwatch
def read_table_lazy_specfic(lake):
    print("Reading specific data from 'my_table' lazily via SQL query...")
    ldf = lake.get_lazy_frame("my_table")
    ldf = ldf.sql(
        "SELECT sequence, id, value_2 FROM self where sequence > 100 and sequence < 200"
    )
    result = ldf.collect()
    print(result)
    return result


@stopwatch
def list_tables(lake):
    print("Listing all tables in the lake...")
    tables = lake.list_tables()
    print(f"Tables found: {tables}")
    return tables


@stopwatch
def get_table_info(lake):
    print("Getting table info for 'my_table'...")
    table_info = lake.get_table_info("my_table")
    print(f"Table info: {table_info}")
    return table_info


@stopwatch
def get_table_schema(lake):
    print("Getting table schema for 'my_table'...")
    table_schema = lake.get_table_schema("my_table")
    print(f"Table schema: {table_schema}")
    return table_schema


@stopwatch
def optimize_table(lake):
    print("Optimizing 'my_table' to improve read performance...")
    print("This compacts small files and may create file statistics")
    lake.optimize_table("my_table")
    print("Optimization completed")


@stopwatch
def vacuum_table(lake):
    print("Vacuuming 'my_table' to clean up old file versions...")
    print("This removes files no longer needed by the table")
    lake.vacuum_table("my_table")
    print("Vacuum operation completed")


@stopwatch
def overwrite_table(lake, df):
    print(f"Overwriting 'my_table' with {len(df):,} new rows...")
    print("This will replace all existing data in the table")
    lake.overwrite_table("my_table", df)
    print("Overwrite operation completed")


@stopwatch
def delete_table(lake):
    print("Deleting 'my_table' from the lake...")
    lake.delete_table("my_table")
    print("Table deleted successfully")


def cleanup():
    """Clean up test directories."""
    print("Cleaning up test directories...")
    data_dir = "examples/example_data/full_example"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    print("Cleanup completed")


if __name__ == "__main__":
    print("=== DELTA LAKE EXAMPLE WORKFLOW ===")
    print("This example demonstrates the full lifecycle of a Delta Lake table")

    print("\n0. Cleanup old test data")
    cleanup()

    print("\n1. Data Preparation")
    full_df = generate_test_data()

    first_half, second_half, overlap_count = split_data_with_overlap(full_df, 0.2)

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

    print("\n4. Table Creation and Initial Data Load")
    create_table(lake, schema)
    write_table(lake, first_half)
    initial_data = read_table(lake)
    print(f"Initial data load: {len(initial_data):,} rows")

    print("\n5. Merge Operations with Overlapping Data")
    print(f"Merging second half with {overlap_count:,} overlapping rows...")
    merge_table(lake, second_half)
    merged_data = read_table_post_merge(lake)

    expected_total = len(full_df)
    actual_total = len(merged_data)

    print("\nMerge Results:")
    print(f"  - Initial data count: {len(initial_data):,} rows")
    print(f"  - Second batch size: {len(second_half):,} rows")
    print(f"  - Overlapping rows: {overlap_count:,}")
    print(f"  - Final row count: {actual_total:,} (Expected: ~{expected_total:,})")
    print(
        f"  - Added ~{actual_total - len(initial_data):,} new rows while avoiding duplicates"
    )

    print("\n6. Table Information")
    list_tables(lake)
    get_table_info(lake)
    get_table_schema(lake)

    print("\n7. Maintenance Operations")
    optimize_table(lake)
    vacuum_table(lake)

    print("\n8. Read Table Lazy")
    read_table_lazy(lake)

    print("\n9. Read Table Lazy Specific")
    read_table_lazy_specfic(lake)

    print("\n10. Table Overwrite Test")
    small_df = full_df.slice(0, 100)

    before_overwrite = read_table(lake)
    print(f"Row count before overwrite: {len(before_overwrite):,}")

    overwrite_table(lake, small_df)

    after_overwrite = read_table(lake)
    print(f"Row count after overwrite: {len(after_overwrite):,}")
    print("Note: The entire table has been replaced with the new smaller dataset")

    print("\n11. Delete Table")
    delete_table(lake)
    list_tables(lake)

    print("\n=== EXAMPLE COMPLETED ===")
