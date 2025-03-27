import os
import shutil
from pdldb import LocalBackupManager
from examples.example_utils.stopwatch import stopwatch


@stopwatch
def initialize_backup_manager():
    print("Initializing Local Backup Manager...")
    backup_dir = os.path.abspath("examples/example_data/")
    os.makedirs(backup_dir, exist_ok=True)

    manager = LocalBackupManager(
        backup_directory=backup_dir, prefix="local_backup_example/"
    )
    print(
        f"Backup Manager initialized successfully with backup directory: {backup_dir}"
    )
    return manager


@stopwatch
def prepare_test_data(data_dir):
    print(f"Creating test data in {data_dir}...")
    os.makedirs(data_dir, exist_ok=True)

    for i in range(1, 4):
        with open(os.path.join(data_dir, f"file{i}.txt"), "w") as f:
            f.write(f"This is test file {i} with some content.\n")
            f.write("It contains multiple lines.\n")

    nested_dir = os.path.join(data_dir, "nested")
    os.makedirs(nested_dir, exist_ok=True)

    with open(os.path.join(nested_dir, "nested_file.txt"), "w") as f:
        f.write("This file is in a nested directory.\n")

    print(f"Created test data with {3 + 1} files")
    return data_dir


@stopwatch
def create_full_backup(manager, source_path):
    print(f"Creating full backup of {source_path}...")
    backup_name = manager.full_backup(source_path)
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
def modify_test_data(data_dir):
    print(f"Modifying test data in {data_dir}...")

    with open(os.path.join(data_dir, "file1.txt"), "a") as f:
        f.write("This line was added to test mirror backups.\n")

    with open(os.path.join(data_dir, "new_file.txt"), "w") as f:
        f.write("This is a new file for the mirror backup test.\n")

    file2_path = os.path.join(data_dir, "file2.txt")
    if os.path.exists(file2_path):
        os.remove(file2_path)
        print(f"Deleted {file2_path}")

    with open(os.path.join(data_dir, "nested", "nested_file.txt"), "a") as f:
        f.write("Added a new line to the nested file.\n")

    print("Test data modified successfully")
    return data_dir


@stopwatch
def create_mirror_backup(manager, source_path):
    print("Creating mirror backup...")
    backup_name = manager.mirror_backup(source_path)
    print(f"mirror backup created successfully: {backup_name}")
    return backup_name


@stopwatch
def restore_backup(manager, backup_name, restore_dir):
    print(f"Restoring backup '{backup_name}' to {restore_dir}...")
    os.makedirs(restore_dir, exist_ok=True)
    success = manager.restore(backup_name, restore_dir)

    if success:
        print(f"Backup restored successfully to {restore_dir}")
        print("Restored files:")
        for root, dirs, files in os.walk(restore_dir):
            for file in files:
                print(f"  - {os.path.join(root, file)}")
    else:
        print("Failed to restore backup")

    return success


@stopwatch
def restore_specific_files(manager, backup_name, restore_dir, specific_files):
    print(f"Restoring specific files from backup '{backup_name}' to {restore_dir}...")
    print(f"Files to restore: {specific_files}")

    os.makedirs(restore_dir, exist_ok=True)
    success = manager.restore(backup_name, restore_dir, specific_files=specific_files)

    if success:
        print(f"Specific files restored successfully to {restore_dir}")
        print("Restored files:")
        for root, dirs, files in os.walk(restore_dir):
            for file in files:
                print(f"  - {os.path.join(root, file)}")
    else:
        print("Failed to restore specific files")

    return success


def cleanup(data_dirs):
    print("Cleaning up test directories...")
    for dir_path in data_dirs:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
    print("Cleanup completed")


if __name__ == "__main__":
    print("=== LOCAL BACKUP MANAGER EXAMPLE WORKFLOW ===")
    print(
        "This example demonstrates the full lifecycle of backups using LocalBackupManager"
    )

    base_dir = "examples/example_data/local_backup_example"
    data_dir = os.path.join(base_dir, "source_data")
    restore_dir_mir_pre_mod = os.path.join(base_dir, "restore_mirror_pre_mod")
    restore_dir_full = os.path.join(base_dir, "restore_full")
    restore_dir_mir_post_mod = os.path.join(base_dir, "restore_mirror_post_mod")
    restore_dir_specific = os.path.join(base_dir, "restore_specific")

    try:
        print("\n0. Cleanup old test data")
        cleanup([base_dir])

        print("\n1. Setup")
        manager = initialize_backup_manager()
        prepare_test_data(data_dir)

        print("\n2. Create Initial mirror Backup")
        mirror_backup_name = create_mirror_backup(manager, data_dir)
        list_available_backups(manager)

        print("\n3. Full Backup (for Disaster Recovery)")
        full_backup_name = create_full_backup(manager, data_dir)
        list_available_backups(manager)

        print("\n4. Restore mirror Backup Before Modification")
        restore_backup(manager, mirror_backup_name, restore_dir_mir_pre_mod)

        print("\n5. Modify Data")
        modify_test_data(data_dir)

        print("\n6. Update mirror Backup")
        mirror_backup_name = create_mirror_backup(manager, data_dir)
        list_available_backups(manager)

        print("\n7. Restore Full Backup")
        restore_backup(manager, full_backup_name, restore_dir_full)

        print("\n8. Restore mirror Backup")
        restore_backup(manager, mirror_backup_name, restore_dir_mir_post_mod)

        print("\n9. Restore Specific Files")
        specific_files = ["file1.txt", "nested/nested_file.txt"]
        restore_specific_files(
            manager, mirror_backup_name, restore_dir_specific, specific_files
        )

    finally:
        print("\n=== EXAMPLE COMPLETED ===")
