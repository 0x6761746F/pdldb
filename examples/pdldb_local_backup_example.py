import os
import shutil
import polars as pl
from pathlib import Path
from pdldb import LocalLakeManager, LocalBackupManager
from examples.example_utils.synth_data import generate_synthetic_data
from examples.example_utils.stopwatch import stopwatch


@stopwatch
def generate_test_data(filename="synthetic_data_local.parquet"):
    print("Starting data generation or loading...")
    data_path = Path(f"examples/example_data/{filename}")
    if not data_path.exists():
        print("Data file not found - generating synthetic data...")
        generate_synthetic_data(target_size_mb=16, output_file=str(data_path))

    df = pl.read_parquet(data_path)
    print(f"Number of rows loaded: {len(df):,}")
    return df


@stopwatch
def initialize_lake(lake_path):
    print(f"Initializing Delta Lake at '{lake_path}'...")
    return LocalLakeManager(lake_path)


@stopwatch
def create_and_write_table(lake, df, table_name="transactions"):
    print(f"Creating table '{table_name}' and writing initial data...")

    schema = {
        "sequence": pl.Int32,
        "id": pl.Int64,
        "value_1": pl.Float32,
        "value_2": pl.Float32,
        "value_3": pl.Utf8,
        "value_4": pl.Float32,
        "value_5": pl.Datetime("ns"),
    }

    lake.create_table(table_name, schema, primary_keys=["sequence", "value_5"])
    lake.append_table(table_name, df)

    print(f"Successfully wrote {len(df):,} rows to '{table_name}'")


@stopwatch
def initialize_backup_manager(backup_dir):
    print(f"Initializing Local Backup Manager at '{backup_dir}'...")

    os.makedirs(backup_dir, exist_ok=True)
    manager = LocalBackupManager(
        backup_directory=backup_dir, prefix="pdldb_local_backup/"
    )

    print("Backup Manager initialized successfully")
    return manager


@stopwatch
def split_data_with_overlap(df, overlap_percentage=0.2):
    print(f"Splitting data with {overlap_percentage * 100}% overlap...")

    total_rows = len(df)
    halfway = total_rows // 2
    overlap_size = int(total_rows * overlap_percentage)
    first_half = df.head(halfway + overlap_size)
    second_half = df.tail(total_rows - halfway + overlap_size)
    first_keys = set(
        zip(first_half["sequence"].to_list(), first_half["value_5"].to_list())
    )
    second_keys = set(
        zip(second_half["sequence"].to_list(), second_half["value_5"].to_list())
    )
    overlap_keys = first_keys.intersection(second_keys)

    print("Split complete:")
    print(f"  - First half: {len(first_half):,} rows")
    print(f"  - Second half: {len(second_half):,} rows")
    print(f"  - Overlapping rows: {len(overlap_keys):,}")

    return first_half, second_half, len(overlap_keys)


@stopwatch
def merge_updates(lake, update_df, table_name="transactions"):
    print(f"Merging {len(update_df):,} rows into '{table_name}'...")
    lake.merge_table(table_name, update_df)
    print("Merge operation completed successfully")


@stopwatch
def create_mirror_backup(backup_manager, source_path):
    print(f"Creating mirror backup of '{source_path}'...")
    backup_name = backup_manager.mirror_backup(source_path)
    print(f"mirror backup created successfully: {backup_name}")
    return backup_name


@stopwatch
def create_full_backup(backup_manager, source_path):
    print(f"Creating full backup of '{source_path}'...")
    backup_name = backup_manager.full_backup(source_path)
    print(f"Full backup created successfully: {backup_name}")
    return backup_name


@stopwatch
def list_available_backups(manager):
    print("Listing available backups...")
    backups = manager.list_backups()
    print(f"Found {len(backups)} backups:")
    for backup in backups:
        if backup.type == "mirror":
            print(
                f"  - {backup.name} (Type: {backup.type}, Created: {backup.created_at}, Source Directory: {backup.source_directory or 'unknown'})"
            )
        else:
            print(
                f"  - {backup.name} (Type: {backup.type}, Created: {backup.created_at})"
            )
    return backups


@stopwatch
def restore_backup(backup_manager, backup_name, restore_path):
    print(f"Restoring backup '{backup_name}' to '{restore_path}'...")

    base_dir = os.path.dirname(restore_path)
    os.makedirs(base_dir, exist_ok=True)

    if os.path.exists(restore_path):
        shutil.rmtree(restore_path)

    success = backup_manager.restore(backup_name, base_dir)

    if success:
        print(f"Backup restored successfully to {restore_path}")
    else:
        print("Failed to restore backup")

    return success


@stopwatch
def read_from_restored_lake(lake_path, table_name="transactions"):
    print(f"Reading data from restored lake at '{lake_path}'...")
    lake = LocalLakeManager(lake_path)

    if table_name not in lake.list_tables():
        print(f"Error: Table '{table_name}' does not exist in the restored lake")
        return None

    df = lake.get_data_frame(table_name)
    print(f"Successfully read {len(df):,} rows from restored table")
    print(df.head(5))

    return df


def cleanup(paths):
    print("Cleaning up test directories...")
    for path in paths:
        if os.path.exists(path):
            shutil.rmtree(path)
    print("Cleanup completed")


if __name__ == "__main__":
    print("=== LOCAL DELTA LAKE BACKUP AND RESTORE EXAMPLE ===")

    base_dir = "examples/example_data/local_backup_delta_example"
    archives_path = f"{base_dir}/archives"
    source_lake_path = f"{archives_path}/source_lake"
    backups_path = f"{base_dir}/backups"
    full_restore_path = f"{base_dir}/restore_full/archives"
    mir_post_merge_path = f"{base_dir}/restore_mirror_post_merge/archives"
    mir_pre_merge_path = f"{base_dir}/restore_mirror_pre_merge/archives"

    try:
        print("\n0. Cleanup old test data")
        cleanup([base_dir])

        print("\n1. Data Preparation")
        original_df = generate_test_data()

        first_half, second_half, overlap_count = split_data_with_overlap(
            original_df, 0.2
        )

        print("\n2. Delta Lake Setup")
        source_lake = initialize_lake(source_lake_path)
        create_and_write_table(source_lake, first_half)

        print("\n3. Backup Manager Setup")
        backup_mgr = initialize_backup_manager(backups_path)

        print("\n4. mirror Backup (Pre-Merge)")
        mir_backup_name_pre_merge = create_mirror_backup(backup_mgr, archives_path)
        list_available_backups(backup_mgr)

        print("\n5. Full Backup")
        full_backup_name = create_full_backup(backup_mgr, archives_path)
        list_available_backups(backup_mgr)

        print("\n6. Restore mirror Backup Pre-Merge")
        restore_backup(backup_mgr, mir_backup_name_pre_merge, mir_pre_merge_path)
        mir_pre_merge_df = read_from_restored_lake(f"{mir_pre_merge_path}/source_lake")
        print(
            f"Pre-merge mirror backup restored data count: {len(mir_pre_merge_df):,} rows (should match first half)"
        )

        print("\n7. Data Addition with Merge")
        print(f"Adding second half of data with {overlap_count:,} overlapping rows...")
        merge_updates(source_lake, second_half)

        final_df = source_lake.get_data_frame("transactions")
        expected_total = len(original_df)
        actual_total = len(final_df)
        print(f"Final row count: {actual_total:,} (Expected: ~{expected_total:,})")
        print(
            f"Added ~{actual_total - len(first_half):,} new rows while avoiding duplicates"
        )

        print("\n8. Post-Merge mirror Backup")
        mir_backup_name_post_merge = create_mirror_backup(backup_mgr, archives_path)
        list_available_backups(backup_mgr)

        print("\n9. Restore Full Backup")
        restore_backup(backup_mgr, full_backup_name, full_restore_path)
        full_restore_df = read_from_restored_lake(f"{full_restore_path}/source_lake")
        print(
            f"Full backup restored data count: {len(full_restore_df):,} rows (matches first half)"
        )

        print("\n10. Restore mirror Backup Post-Merge")
        restore_backup(backup_mgr, mir_backup_name_post_merge, mir_post_merge_path)
        mir_restore_df = read_from_restored_lake(f"{mir_post_merge_path}/source_lake")
        print(
            f"Post-merge mirror backup restored data count: {len(mir_restore_df):,} rows (matches complete dataset)"
        )

        print("\n11. Data Validation")
        if (
            full_restore_df is not None
            and mir_restore_df is not None
            and mir_pre_merge_df is not None
        ):
            print("\n=== DATA VALIDATION SUMMARY ===")

            # Dataset sizes
            print("\n DATASET METRICS")
            print(f"  • Original dataset:                 {len(original_df):,} rows")
            print(f"  • Initial data load (first half):   {len(first_half):,} rows")
            print(f"  • Second data batch:                {len(second_half):,} rows")
            print(f"    - With overlapping rows:          {overlap_count:,} rows")

            # Backup results
            print("\n BACKUP VALIDATION")
            print(f"  • Pre-merge mirror backup:     {len(mir_pre_merge_df):,} rows")
            print(
                f"  • Pre-merge full backup restored:   {len(full_restore_df):,} rows"
            )
            print(f"  • Post-merge mirror backup:    {len(mir_restore_df):,} rows")

            # Validation checks
            print("\n VALIDATION CHECKS")

            # Check 1: Pre-merge validation
            pre_merge_diff = abs(len(mir_pre_merge_df) - len(first_half))
            if pre_merge_diff == 0:
                print(
                    "  ✓ PRE-MERGE CHECK: Successful - mirror backup exactly matches initial data load"
                )
            else:
                print(
                    f"  ✗ PRE-MERGE CHECK: Failed - Backup row count differs from initial data by {pre_merge_diff:,} rows"
                )

            # Check 2: Post-merge validation
            new_rows_count = len(mir_restore_df) - len(full_restore_df)
            expected_new_rows = len(second_half) - overlap_count
            post_merge_diff = abs(new_rows_count - expected_new_rows)

            print(f"  • New rows after merge:             {new_rows_count:,} rows")
            print(f"  • Expected new rows:                {expected_new_rows:,} rows")

            if post_merge_diff == 0:
                print(
                    "  ✓ POST-MERGE CHECK: Successful - mirror backup contains exactly the expected number of new rows"
                )
            else:
                print(
                    f"  ✗ POST-MERGE CHECK: Failed - New row count differs from expected by {post_merge_diff:,} rows"
                )

            # Overlap confirmation
            expected_overlaps = len(mir_restore_df) == len(original_df)
            if expected_overlaps:
                print(
                    "  ✓ COMPLETENESS CHECK: Successful - Restored data matches original dataset size"
                )
            else:
                diff = abs(len(mir_restore_df) - len(original_df))
                print(
                    f"  ✗ COMPLETENESS CHECK: Failed - Restored data size differs from original by {diff:,} rows"
                )

            print("\n=== END OF VALIDATION ===")

    finally:
        print("\n=== EXAMPLE COMPLETED ===")
