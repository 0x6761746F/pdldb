import os
import tarfile
import shutil
import hashlib
import json
from datetime import datetime
from typing import Dict, List, Optional, Union, Set
from pydantic import BaseModel, Field, DirectoryPath


class BackupInfo(BaseModel):
    name: str
    type: str
    created_at: str
    source_directory: Optional[str] = None


class FileInfo(BaseModel):
    hash: str
    mtime: float


class ManifestModel(BaseModel):
    files: Dict[str, FileInfo] = {}
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    type: Optional[str] = None
    source_directory: Optional[str] = None


class FullBackupParams(BaseModel):
    source_path: Union[str, os.PathLike]
    backup_name: Optional[str] = None


class MirrorBackupParams(BaseModel):
    source_path: Union[str, os.PathLike]


class RestoreParams(BaseModel):
    backup_name: str
    destination_path: Union[str, os.PathLike]
    specific_files: Optional[Set[str]] = None


class LocalBackupManager:
    """
    Manages local file backups with support for both full and mirror backup strategies.
    
    This class provides functionality to:

    - Create full backups (complete tar.gz archives with manifests)
    - Create/update mirror backups (individual files with change detection)
    - Restore files from either backup type
    - List available backups with their metadata
    
    All backups are stored in a structured directory hierarchy:

    - {backup_directory}/{prefix}full_backups/ - Contains all full backups
    - {backup_directory}/{prefix}mirror_backup/ - Contains the single mirror backup
    
    Each backup includes a manifest.json file with metadata about the backup, including file hashes, modification times, creation timestamp, and backup type.
    """
    class Config(BaseModel):
        backup_directory: DirectoryPath
        prefix: str = "pdldb_backups/"

    def __init__(
        self, backup_directory: Union[str, os.PathLike], prefix: str = "pdldb_backups/"
    ):
        """
        Initialize a LocalBackupManager with the specified backup directory and prefix.
        
        Args:
            backup_directory: The base directory where all backups will be stored
            prefix: Optional prefix for backup subdirectories (default: "pdldb_backups/")
                Used to organize backups within the backup_directory
        
        !!! note
            This method creates the necessary subdirectories for both full and mirror 
            backups if they don't already exist.
        """
        config = self.Config(
            backup_directory=os.path.abspath(backup_directory), prefix=prefix
        )
        self.backup_directory = config.backup_directory
        self.prefix = config.prefix
        self.full_prefix = os.path.join(self.backup_directory, f"{prefix}full_backups/")
        self.mir_prefix = os.path.join(self.backup_directory, f"{prefix}mirror_backup/")
        os.makedirs(self.full_prefix, exist_ok=True)
        os.makedirs(self.mir_prefix, exist_ok=True)

    def _get_file_hash(self, filepath: Union[str, os.PathLike]) -> Optional[str]:
        if not os.path.isfile(filepath):
            return None
        hasher = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _get_manifest_path(self, backup_name: str) -> str:
        return os.path.join(self.full_prefix, backup_name, "manifest.json")

    def _get_mir_manifest_path(self) -> str:
        return os.path.join(self.mir_prefix, "manifest.json")

    def _load_manifest(self, backup_name: Optional[str] = None) -> ManifestModel:
        if backup_name:
            manifest_path = self._get_manifest_path(backup_name)
        else:
            manifest_path = self._get_mir_manifest_path()

        if os.path.exists(manifest_path):
            with open(manifest_path, "r") as f:
                data = json.load(f)
                return ManifestModel(**data)
        return ManifestModel(files={}, created_at=datetime.now().isoformat())

    def _save_manifest(
        self, manifest: ManifestModel, backup_name: Optional[str] = None
    ) -> None:
        if backup_name:
            manifest_path = self._get_manifest_path(backup_name)
            os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
        else:
            manifest_path = self._get_mir_manifest_path()

        with open(manifest_path, "w") as f:
            json.dump(manifest.model_dump(), f, indent=2)

    def full_backup(
        self, source_path: Union[str, os.PathLike], backup_name: Optional[str] = None
    ) -> str:
        """
        Creates a complete backup of a source directory as a compressed archive.
        
        A full backup creates a new backup directory containing:

        - A compressed tar.gz archive of the entire source directory
        - A manifest.json file with metadata and file hashes for all files
        
        Unlike mirror backups, each full backup is stored as a separate archive, allowing for multiple backup versions to be maintained.
        
        Args:
            source_path: Path to the directory that should be backed up
            backup_name: Optional custom name for the backup. If not provided,
                        a name will be generated using the source directory name
                        and current timestamp (e.g., "mydir_20250325_123045")
            
        Returns:
            str: The name of the created backup (either the provided backup_name
                or the auto-generated name)
                
        !!! note
            - If a backup with the specified name already exists, its contents will be overwritten.
            - The manifest includes SHA-256 hashes and modification times for all files,
            which can be used for verification or restoration purposes.
        
        Example:
            ```python
            manager = LocalBackupManager(backup_directory="/path/to/backups/")
            backup_name = manager.full_backup(source_path="/path/to/source_dir/")
            print(backup_name)
            # Output: "source_dir_20250325_123045"
            ```
        """
        params = FullBackupParams(source_path=source_path, backup_name=backup_name)

        source_path = os.path.abspath(params.source_path)
        source_dir = os.path.basename(source_path)

        if not params.backup_name:
            date_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{source_dir}_{date_suffix}"
        else:
            backup_name = params.backup_name

        backup_dir = os.path.join(self.full_prefix, backup_name)
        os.makedirs(backup_dir, exist_ok=True)
        archive_path = os.path.join(backup_dir, "full_backup.tar.gz")

        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(source_path, arcname=os.path.basename(source_path))

        manifest = ManifestModel(
            files={}, created_at=datetime.now().isoformat(), type="full"
        )

        for root, _, files in os.walk(source_path):
            for file in files:
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, source_path)
                manifest.files[rel_path] = FileInfo(
                    hash=self._get_file_hash(file_path) or "",
                    mtime=os.path.getmtime(file_path),
                )

        self._save_manifest(manifest, backup_name)
        return backup_name

    def mirror_backup(self, source_path: Union[str, os.PathLike]) -> str:
        """
        Creates or updates a mirror backup from the source directory.
        
        A mirror backup differs from a full backup in several ways:

        - Only one mirror backup can exist at a time (in the mirror_backup directory)
        - Files are stored individually rather than in a tar archive
        - Only files that have changed (based on hash comparison) are copied
        - Files in the backup that no longer exist in the source are removed
        
        Args:
            source_path: Path to the directory that should be backed up
            
        Returns:
            str: Always returns "mirror_backup" as the backup identifier
            
        !!! note
            The backup's source directory name is stored in the manifest to help with
            restoration. Empty directories in the backup that result from file deletions
            are automatically removed.
        
        Example:
            ```python
            manager = LocalBackupManager(backup_directory="/path/to/backups/")
            backup_name = manager.mirror_backup(source_path="/path/to/source_dir/")
            print(backup_name)
            # Output: "mirror_backup"
            ```
        """
        params = MirrorBackupParams(source_path=source_path)
        source_path = os.path.abspath(params.source_path)
        source_dir = os.path.basename(source_path)

        os.makedirs(self.mir_prefix, exist_ok=True)
        current_mir_manifest = self._load_manifest()

        local_stored_files = {}
        for root, _, files in os.walk(self.mir_prefix):
            for file in files:
                if file == "manifest.json":
                    continue
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, self.mir_prefix)
                local_stored_files[rel_path] = file_path

        local_files = {}
        for root, _, files in os.walk(source_path):
            for file in files:
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, source_path)
                file_hash = self._get_file_hash(file_path) or ""
                local_files[rel_path] = {
                    "path": file_path,
                    "hash": file_hash,
                    "mtime": os.path.getmtime(file_path),
                }

        to_upload = []
        to_delete = []

        for rel_path, file_info in local_files.items():
            if (
                rel_path not in current_mir_manifest.files
                or file_info["hash"]
                != current_mir_manifest.files.get(
                    rel_path, FileInfo(hash="", mtime=0)
                ).hash
            ):
                to_upload.append((file_info["path"], rel_path))

        for rel_path in local_stored_files:
            if rel_path not in local_files:
                to_delete.append(local_stored_files[rel_path])

        new_manifest = ManifestModel(
            files={},
            created_at=datetime.now().isoformat(),
            type="mirror",
            source_directory=source_dir,
        )

        for rel_path, file_info in local_files.items():
            new_manifest.files[rel_path] = FileInfo(
                hash=file_info["hash"], mtime=file_info["mtime"]
            )

        for file_path, rel_path in to_upload:
            target_path = os.path.join(self.mir_prefix, rel_path)
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            try:
                shutil.copy2(file_path, target_path)
            except Exception as e:
                print(f"Error copying {file_path}: {e}")

        for file_path in to_delete:
            try:
                os.remove(file_path)
                dir_path = os.path.dirname(file_path)
                while dir_path != self.mir_prefix:
                    if not os.listdir(dir_path):
                        os.rmdir(dir_path)
                        dir_path = os.path.dirname(dir_path)
                    else:
                        break
            except Exception as e:
                print(f"Error deleting {file_path}: {e}")

        self._save_manifest(new_manifest)
        return "mirror_backup"

    def restore(
        self,
        backup_name: str,
        destination_path: Union[str, os.PathLike],
        specific_files: Optional[List[str]] = None,
    ) -> bool:
        """
        Restores files from a backup to a specified destination path.
        
        This method supports restoring from both full and mirror backups:

        - For mirror backups: Files are copied individually while preserving the directory structure
        - For full backups: The tar.gz archive is extracted to the destination
        
        Args:
            backup_name: The name of the backup to restore from. Use "mirror_backup" for mirror backups or the directory name for full backups.
            destination_path: The target directory where files will be restored to.
            specific_files: Optional list of specific file paths to restore. If None, all files will be restored. Paths should be relative to the original backup source.
        
        Returns:
            bool: True if restoration succeeded, False if it failed.
        
        !!! note
            - For mirror backups, if the original source directory is stored in the manifest, a subdirectory with that name will be created at the destination.
            - For full backups, the entire archive is extracted even when specific_files is used.
            - Any errors during restoration are logged to stdout and will cause the method to return False.
        
        Example:
            ```python
            manager = LocalBackupManager(backup_directory="/path/to/backups/")
            success = manager.restore(backup_name="my_backup", destination_path="/path/to/restore_dir/")
            print(success)
            # Output: True
            ```
        """      
        params = RestoreParams(
            backup_name=backup_name,
            destination_path=destination_path,
            specific_files=set(specific_files) if specific_files else None,
        )

        os.makedirs(params.destination_path, exist_ok=True)

        if params.backup_name == "mirror_backup":
            manifest = self._load_manifest()
            source_dir = manifest.source_directory or ""

            if source_dir:
                target_path = os.path.join(params.destination_path, source_dir)
                os.makedirs(target_path, exist_ok=True)
            else:
                target_path = params.destination_path

            try:
                for root, _, files in os.walk(self.mir_prefix):
                    for file in files:
                        if file == "manifest.json":
                            continue
                        file_path = os.path.join(root, file)
                        rel_path = os.path.relpath(file_path, self.mir_prefix)

                        if (
                            params.specific_files
                            and rel_path not in params.specific_files
                        ):
                            continue

                        if source_dir:
                            local_path = os.path.join(target_path, rel_path)
                        else:
                            local_path = os.path.join(params.destination_path, rel_path)

                        os.makedirs(os.path.dirname(local_path), exist_ok=True)
                        shutil.copy2(file_path, local_path)
                return True
            except Exception as e:
                print(f"Error during mirror restore: {e}")
                return False

        backup_dir = os.path.join(self.full_prefix, params.backup_name)
        archive_path = os.path.join(backup_dir, "full_backup.tar.gz")

        if not os.path.exists(archive_path):
            print(f"Backup archive not found: {archive_path}")
            return False

        try:
            with tarfile.open(archive_path, "r:gz") as tar:
                if params.specific_files:
                    for member in tar.getmembers():
                        if member.name in params.specific_files:
                            tar.extractall(path=params.destination_path, filter="data")
                else:
                    tar.extractall(path=params.destination_path, filter="data")
            return True
        except Exception as e:
            print(f"Error during full backup restore: {e}")
            return False

    def list_backups(self) -> List[BackupInfo]:
        """
        Lists all available backups in the backup directory.
        
        This method scans both full backups and mirror backups:

        - Full backups: Individual directories in the full_prefix location, each with a manifest.json
        - Mirror backup: A single backup in the mirror_backup location with its own manifest.json
        
        Returns:
            List[BackupInfo]: A list of BackupInfo objects containing details about each backup:
                - name: The name of the backup (directory name for full backups, "mirror_backup" for mirror)
                - type: Either "full" or "mirror"
                - created_at: ISO timestamp when the backup was created
                - source_directory: Original source directory name (only for mirror backups)
        
        !!! note
            If errors occur while reading manifests, they are logged to stdout but don't interrupt 
            the listing process.

        Example:
            ```python
            manager = LocalBackupManager(backup_directory="/path/to/backups/")
            backups = manager.list_backups()
            for backup in backups:
                print(f"{backup.name} ({backup.type}) - Created: {backup.created_at}")
            ```
        """
        backups = []
        try:
            for item in os.listdir(self.full_prefix):
                item_path = os.path.join(self.full_prefix, item)
                if os.path.isdir(item_path):
                    manifest_path = os.path.join(item_path, "manifest.json")
                    if os.path.exists(manifest_path):
                        try:
                            with open(manifest_path, "r") as f:
                                manifest = json.load(f)
                                backups.append(
                                    BackupInfo(
                                        name=item,
                                        type="full",
                                        created_at=manifest.get(
                                            "created_at", "unknown"
                                        ),
                                    )
                                )
                        except Exception as e:
                            print(f"Error reading manifest for {item}: {e}")
        except Exception as e:
            print(f"Error listing full backups: {e}")

        mir_manifest_path = self._get_mir_manifest_path()
        if os.path.exists(mir_manifest_path):
            try:
                with open(mir_manifest_path, "r") as f:
                    manifest = json.load(f)
                    if manifest.get("files"):
                        backups.append(
                            BackupInfo(
                                name="mirror_backup",
                                type="mirror",
                                created_at=manifest.get("created_at", "unknown"),
                                source_directory=manifest.get(
                                    "source_directory", "unknown"
                                ),
                            )
                        )
            except Exception as e:
                print(f"Error reading mirror backup manifest: {e}")

        return backups
