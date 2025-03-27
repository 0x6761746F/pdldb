import os
import pytest
import shutil
import tempfile
from pathlib import Path

from pdldb.local_backup_manager import LocalBackupManager


@pytest.fixture
def temp_dir():
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_file_content():
    return "This is test content for backup testing."


@pytest.fixture
def source_dir(temp_dir, sample_file_content):
    source_path = Path(temp_dir) / "source"
    source_path.mkdir()

    for i in range(3):
        file_path = source_path / f"test_file_{i}.txt"
        with open(file_path, "w") as f:
            f.write(f"{sample_file_content} File {i}")

    subdir = source_path / "subdir"
    subdir.mkdir()
    for i in range(2):
        file_path = subdir / f"subdir_file_{i}.txt"
        with open(file_path, "w") as f:
            f.write(f"{sample_file_content} Subdir File {i}")

    return source_path


@pytest.fixture
def backup_dir(temp_dir):
    backup_path = Path(temp_dir) / "backups"
    backup_path.mkdir()
    return backup_path


@pytest.fixture
def backup_manager(backup_dir):
    return LocalBackupManager(backup_directory=str(backup_dir))


class TestLocalBackupManager:
    def test_initialization(self, backup_dir):
        manager = LocalBackupManager(
            backup_directory=str(backup_dir), prefix="test_prefix/"
        )

        assert os.path.abspath(str(backup_dir)) == str(manager.backup_directory)
        assert manager.prefix == "test_prefix/"

    def test_full_backup(self, backup_manager, source_dir):
        backup_name = backup_manager.full_backup(source_path=str(source_dir))

        backup_dir = os.path.join(backup_manager.full_prefix, backup_name)
        archive_path = os.path.join(backup_dir, "full_backup.tar.gz")
        assert os.path.exists(archive_path)

        manifest_path = os.path.join(backup_dir, "manifest.json")
        assert os.path.exists(manifest_path)

    def test_mirror_backup(self, backup_manager, source_dir):
        backup_name = backup_manager.mirror_backup(source_path=str(source_dir))

        assert backup_name == "mirror_backup"

        manifest_path = backup_manager._get_mir_manifest_path()
        assert os.path.exists(manifest_path)

        for file_path in Path(source_dir).glob("**/*.txt"):
            rel_path = file_path.relative_to(source_dir)
            mirror_path = Path(backup_manager.mir_prefix) / rel_path
            assert mirror_path.exists()
            assert mirror_path.read_text() == file_path.read_text()

    def test_list_backups(self, backup_manager, source_dir):
        full_backup_name = backup_manager.full_backup(source_path=str(source_dir))

        backup_manager.mirror_backup(source_path=str(source_dir))

        backups = backup_manager.list_backups()

        assert len(backups) == 2

        backup_types = [b.type for b in backups]
        assert "full" in backup_types
        assert "mirror" in backup_types

        backup_names = [b.name for b in backups]
        assert full_backup_name in backup_names
        assert "mirror_backup" in backup_names

    def test_restore_full_backup(self, backup_manager, source_dir, temp_dir):
        backup_name = backup_manager.full_backup(source_path=str(source_dir))

        restore_dir = Path(temp_dir) / "restore"
        restore_dir.mkdir()

        result = backup_manager.restore(
            backup_name=backup_name, destination_path=str(restore_dir)
        )

        assert result is True

        source_base = source_dir.name
        for file_path in Path(source_dir).glob("**/*.txt"):
            rel_path = file_path.relative_to(source_dir)
            restore_path = restore_dir / source_base / rel_path
            assert restore_path.exists()
            assert restore_path.read_text() == file_path.read_text()

    def test_restore_mirror_backup(self, backup_manager, source_dir, temp_dir):
        backup_manager.mirror_backup(source_path=str(source_dir))

        restore_dir = Path(temp_dir) / "restore"
        restore_dir.mkdir()

        result = backup_manager.restore(
            backup_name="mirror_backup", destination_path=str(restore_dir)
        )

        assert result is True

        source_base = source_dir.name
        for file_path in Path(source_dir).glob("**/*.txt"):
            rel_path = file_path.relative_to(source_dir)
            restore_path = restore_dir / source_base / rel_path
            assert restore_path.exists()
            assert restore_path.read_text() == file_path.read_text()

    # Add test for restore specific file
