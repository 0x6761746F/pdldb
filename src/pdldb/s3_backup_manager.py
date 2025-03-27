import os
import tarfile
import tempfile
import boto3
import hashlib
import json
from datetime import datetime
from botocore.exceptions import ClientError
from pydantic import BaseModel
from typing import Optional, List, Dict, Any


class S3BackupConfig(BaseModel):
    bucket_name: Optional[str] = None
    aws_region: Optional[str] = None
    prefix: str = "pdldb_backups/"
    endpoint_url: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None

class FullBackupRequest(BaseModel):
    source_path: str
    backup_name: Optional[str] = None


class MirrorBackupRequest(BaseModel):
    source_path: str


class RestoreRequest(BaseModel):
    backup_name: str
    destination_path: str
    specific_files: Optional[List[str]] = None


class S3BackupManager:
    """
    Manages file backups to Amazon S3 or compatible storage with support for 
    both full and mirror backup strategies.
    
    This class provides cloud-based functionality to:
    
    - Create full backups (complete tar.gz archives with manifests)
    - Create/update mirror backups (individual files with change detection)
    - Restore files from either backup type
    - List available backups with their metadata
    
    All backups are stored in a structured S3 key hierarchy:
    
    - {prefix}full_backups/ - Contains all full backups
    - {prefix}mirror_backup/ - Contains the single mirror backup
    
    Each backup includes a manifest.json file with metadata about the backup, including 
    file hashes, modification times, creation timestamp, and backup type.
    """
    def __init__(
        self,
        bucket_name: Optional[str] = None,
        aws_region: Optional[str] = None,
        prefix: str = "pdldb_backups/",
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ) -> None:
        """
        Initialize an S3BackupManager with the specified S3 bucket and configuration.
        
        Args:
            bucket_name: S3 bucket where backups will be stored. If None, will use S3_BUCKET_NAME env var.
            aws_region: AWS region for the S3 bucket. If None, will use AWS_REGION or AWS_DEFAULT_REGION env vars.
            prefix: Optional prefix for backup keys in the bucket (default: "pdldb_backups/")
            endpoint_url: Optional endpoint URL for S3-compatible storage (e.g., MinIO)
            aws_access_key_id: AWS access key ID. If None, will use AWS_ACCESS_KEY_ID env var.
            aws_secret_access_key: AWS secret access key. If None, will use AWS_SECRET_ACCESS_KEY env var.
        
        !!! note
            This method sets up the S3 client and prepares the necessary key prefixes for
            both full and mirror backups.

        Example:
            ```python
            backup_manager = S3BackupManager(bucket_name="my-bucket")
            ```
        """
        config = S3BackupConfig(
            bucket_name=bucket_name,
            aws_region=aws_region,
            prefix=prefix,
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

        self.s3_client = boto3.client(
            "s3",
            region_name=config.aws_region
            or os.environ.get("AWS_REGION")
            or os.environ.get("AWS_DEFAULT_REGION"),
            endpoint_url=config.endpoint_url,
            aws_access_key_id=config.aws_access_key_id
            or os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=config.aws_secret_access_key
            or os.environ.get("AWS_SECRET_ACCESS_KEY"),
        )
        self.bucket = config.bucket_name or os.environ.get("S3_BUCKET_NAME")
        self.prefix = config.prefix
        self.full_prefix = f"{config.prefix}full_backups/"
        self.mir_prefix = f"{config.prefix}mirror_backup/"

        if not self.bucket:
            raise ValueError(
                "S3 bucket name is required. Provide it as a parameter or set S3_BUCKET_NAME environment variable."
            )

    def _get_file_hash(self, filepath):
        if not os.path.isfile(filepath):
            return None

        hasher = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _get_manifest_key(self, backup_name):
        return f"{self.full_prefix}{backup_name}/manifest.json"

    def _get_mir_manifest_key(self):
        return f"{self.mir_prefix}manifest.json"

    def _load_manifest(self, backup_name=None):
        if backup_name:
            try:
                response = self.s3_client.get_object(
                    Bucket=self.bucket, Key=self._get_manifest_key(backup_name)
                )
                return json.loads(response["Body"].read().decode("utf-8"))
            except ClientError:
                return {"files": {}, "created_at": datetime.now().isoformat()}
        else:
            try:
                response = self.s3_client.get_object(
                    Bucket=self.bucket, Key=self._get_mir_manifest_key()
                )
                return json.loads(response["Body"].read().decode("utf-8"))
            except ClientError:
                return {"files": {}, "created_at": datetime.now().isoformat()}

    def _save_manifest(self, manifest, backup_name=None):
        manifest_data = json.dumps(manifest).encode("utf-8")
        if backup_name:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=self._get_manifest_key(backup_name),
                Body=manifest_data,
            )
        else:
            self.s3_client.put_object(
                Bucket=self.bucket, Key=self._get_mir_manifest_key(), Body=manifest_data
            )

    def full_backup(self, source_path: str, backup_name: Optional[str] = None) -> str:
        """
        Creates a complete backup of a source directory as a compressed archive in S3.
        
        A full backup creates:
        
        - A compressed tar.gz archive of the entire source directory uploaded to S3
        - A manifest.json file with metadata and file hashes for all files
        
        Unlike mirror backups, each full backup is stored as a separate archive,
        allowing for multiple backup versions to be maintained.
        
        Args:
            source_path: Path to the directory that should be backed up
            backup_name: Optional custom name for the backup. If not provided,
                        a name will be generated using the source directory name
                        and current timestamp (e.g., "mydir_20250325_123045")
            
        Returns:
            str: The name of the created backup (either the provided backup_name
                or the auto-generated name)
                
        !!! note
            - If a backup with the specified name already exists in S3, it will be overwritten.
            - The manifest includes SHA-256 hashes and modification times for all files, which can be used for verification or restoration purposes.
            - The archive is created in a temporary file before being uploaded to S3.

        Example:
            ```python
            backup_manager = S3BackupManager(bucket_name="my-bucket")
            backup_name = backup_manager.full_backup("/path/to/source")
            print(f"Full backup created: {backup_name}")
            ```
        """
        params = FullBackupRequest(source_path=source_path, backup_name=backup_name)

        source_path = os.path.abspath(params.source_path)
        source_dir = os.path.basename(source_path)

        if not os.path.exists(source_path):
            raise ValueError(f"Source path does not exist: {source_path}")

        if not params.backup_name:
            date_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{source_dir}_{date_suffix}"
        else:
            backup_name = params.backup_name

        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp_file:
            archive_path = tmp_file.name

        try:
            with tarfile.open(archive_path, "w:gz") as tar:
                tar.add(source_path, arcname=os.path.basename(source_path))

            backup_key = f"{self.full_prefix}{backup_name}/full_backup.tar.gz"
            self.s3_client.upload_file(
                archive_path,
                self.bucket,
                backup_key,
                ExtraArgs={"StorageClass": "STANDARD"},
            )

            created_at = datetime.now().isoformat()

            manifest = {
                "files": {},
                "created_at": created_at,
                "type": "full",
            }

            file_count = 0
            for root, _, files in os.walk(source_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    rel_path = os.path.relpath(file_path, source_path)
                    manifest["files"][rel_path] = {
                        "hash": self._get_file_hash(file_path),
                        "mtime": os.path.getmtime(file_path),
                    }
                    file_count += 1

            self._save_manifest(manifest, backup_name)

            return backup_name

        finally:
            if os.path.exists(archive_path):
                os.unlink(archive_path)

    def mirror_backup(self, source_path: str) -> str:
        """
        Creates or updates a mirror backup from the source directory to S3.
        
        A mirror backup differs from a full backup in several ways:
        
        - Only one mirror backup can exist at a time (in the mirror_backup prefix)
        - Files are stored individually rather than in a tar archive
        - Only files that have changed (based on hash comparison) are uploaded
        - Files in S3 that no longer exist in the source are removed
        
        The method performs an incremental sync by:
        
        1. Scanning the source directory and calculating file hashes
        2. Comparing with the current S3 mirror backup manifest
        3. Determining which files need to be added, updated, or removed
        4. Uploading only the changed files and updating the manifest
        
        Args:
            source_path: Path to the directory that should be backed up
            
        Returns:
            str: Always returns "mirror_backup" as the backup identifier
            
        !!! note
            - The backup's source directory name is stored in the manifest to help with restoration
            - S3 objects are deleted in batches of 1000 (S3 API limitation)
            - Errors during upload or deletion are logged but don't interrupt the overall process
        
        Example:
            ```python
            backup_manager = S3BackupManager(bucket_name="my-bucket")
            success = backup_manager.mirror_backup("/path/to/source")
            if success:
                print("Mirror backup completed!")
            ```
        """
        params = MirrorBackupRequest(source_path=source_path)
        source_path = os.path.abspath(params.source_path)

        if not os.path.exists(source_path):
            raise ValueError(f"Source path does not exist: {source_path}")

        source_dir = os.path.basename(source_path)

        try:
            current_mir_manifest = self._load_manifest()
        except ClientError:
            current_mir_manifest = {
                "files": {},
                "created_at": datetime.now().isoformat(),
            }

        s3_files = {}
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket, Prefix=self.mir_prefix)

            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not key.endswith("manifest.json"):
                        rel_path = key[len(self.mir_prefix) :]
                        s3_files[rel_path] = key
        except ClientError:
            pass

        local_files = {}
        for root, _, files in os.walk(source_path):
            for file in files:
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, source_path)
                local_files[rel_path] = {
                    "path": file_path,
                    "hash": self._get_file_hash(file_path),
                    "mtime": os.path.getmtime(file_path),
                }

        to_upload = []
        to_delete = []

        for rel_path, file_info in local_files.items():
            if rel_path not in current_mir_manifest.get("files", {}) or file_info[
                "hash"
            ] != current_mir_manifest["files"].get(rel_path, {}).get("hash"):
                to_upload.append((file_info["path"], rel_path))

        for rel_path in s3_files:
            if rel_path not in local_files:
                to_delete.append({"Key": s3_files[rel_path]})

        new_manifest = {
            "files": {},
            "created_at": datetime.now().isoformat(),
            "type": "mirror",
            "source_directory": source_dir,
        }

        for rel_path, file_info in local_files.items():
            new_manifest["files"][rel_path] = {
                "hash": file_info["hash"],
                "mtime": file_info["mtime"],
            }

        for file_path, rel_path in to_upload:
            target_key = f"{self.mir_prefix}{rel_path}"
            try:
                self.s3_client.upload_file(
                    file_path,
                    self.bucket,
                    target_key,
                    ExtraArgs={"StorageClass": "STANDARD"},
                )
            except Exception as e:
                print(f"Error uploading {file_path}: {e}")

        if to_delete:
            try:
                for i in range(0, len(to_delete), 1000):
                    batch = to_delete[i : i + 1000]
                    self.s3_client.delete_objects(
                        Bucket=self.bucket, Delete={"Objects": batch, "Quiet": True}
                    )
            except Exception as e:
                print(f"Error deleting objects: {e}")

        self._save_manifest(new_manifest)

        return "mirror_backup"

    def restore(
        self,
        backup_name: str,
        destination_path: str,
        specific_files: Optional[List[str]] = None,
    ) -> bool:
        """
        Restores files from an S3 backup to a specified local destination path.
        
        This method supports restoring from both full and mirror backups:
        
        - For mirror backups: Files are downloaded individually while preserving the directory structure
        - For full backups: The tar.gz archive is downloaded and extracted to the destination
        
        Args:
            backup_name: The name of the backup to restore from. Use "mirror_backup" for mirror backups
                        or the directory name for full backups.
            destination_path: The target directory where files will be restored to.
            specific_files: Optional list of specific file paths to restore. If None, all files 
                            will be restored. Paths should be relative to the original backup source.
        
        Returns:
            bool: True if restoration succeeded, False if it failed.
        
        !!! note
            - For mirror backups, if the original source directory is stored in the manifest, a subdirectory with that name will be created at the destination.
            - For full backups, the entire archive is downloaded to a temporary directory before extracting the specified files or the entire contents.
            - Any errors during restoration are logged to stdout and will cause the method to return False.

        Example:
            ```python
            backup_manager = S3BackupManager(bucket_name="my-bucket")
            success = backup_manager.restore("my_backup", "/restore/path")
            if success:
                print("Restoration successful!")
            ```
        """
        params = RestoreRequest(
            backup_name=backup_name,
            destination_path=destination_path,
            specific_files=specific_files,
        )

        destination_path = params.destination_path
        backup_name = params.backup_name
        specific_files = params.specific_files

        os.makedirs(destination_path, exist_ok=True)

        if backup_name == "mirror_backup":
            manifest = self._load_manifest()
            files_prefix = self.mir_prefix

            source_dir = manifest.get("source_directory", "")

            if source_dir:
                target_path = os.path.join(destination_path, source_dir)
                os.makedirs(target_path, exist_ok=True)
            else:
                target_path = destination_path

            try:
                paginator = self.s3_client.get_paginator("list_objects_v2")
                pages = paginator.paginate(Bucket=self.bucket, Prefix=files_prefix)

                for page in pages:
                    for obj in page.get("Contents", []):
                        file_key = obj["Key"]
                        if file_key.endswith("manifest.json"):
                            continue

                        rel_path = file_key[len(files_prefix) :]

                        if specific_files and rel_path not in specific_files:
                            continue

                        if source_dir:
                            local_path = os.path.join(target_path, rel_path)
                        else:
                            local_path = os.path.join(destination_path, rel_path)

                        os.makedirs(os.path.dirname(local_path), exist_ok=True)

                        self.s3_client.download_file(self.bucket, file_key, local_path)

                return True
            except ClientError as e:
                print(f"Error during mirror restore: {e}")
                return False

        manifest = self._load_manifest(backup_name)

        with tempfile.TemporaryDirectory() as temp_dir:
            archive_key = f"{self.full_prefix}{backup_name}/full_backup.tar.gz"
            archive_path = os.path.join(temp_dir, f"{backup_name}.tar.gz")

            try:
                self.s3_client.download_file(self.bucket, archive_key, archive_path)

                with tarfile.open(archive_path, "r:gz") as tar:
                    if specific_files:
                        for member in tar.getmembers():
                            if member.name in specific_files:
                                tar.extract(member, path=destination_path)
                    else:
                        tar.extractall(path=destination_path)

                return True

            except ClientError:
                return False

        return False

    def list_backups(self) -> List[Dict[str, Any]]:
        """
        Lists all available backups in the S3 bucket under the configured prefix.
        
        This method scans both full backups and mirror backups:
        
        - Full backups: S3 prefixes under full_backups/, each with a manifest.json
        - Mirror backup: A single backup in the mirror_backup/ prefix with its own manifest.json
        
        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing details about each backup:
                - name: The name of the backup (prefix name for full backups, "mirror_backup" for mirror)
                - type: Either "full" or "mirror"
                - created_at: ISO timestamp when the backup was created
                - source_directory: Original source directory name (only for mirror backups)
        
        !!! note
            If errors occur while fetching objects or parsing manifests, they are logged to stdout
            but don't interrupt the listing process. The S3 list operation uses pagination to handle
            potentially large numbers of backups.

        Example:
            ```python
            backup_manager = S3BackupManager(bucket_name="my-bucket")
            backups = backup_manager.list_backups()
            for backup in backups:
                print(f"{backup['name']} ({backup['type']}): {backup['created_at']}")
            ```
        """
        backups = []

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket, Prefix=self.full_prefix, Delimiter="/"
            )

            for prefix in response.get("CommonPrefixes", []):
                backup_name = prefix["Prefix"].split("/")[-2]
                try:
                    manifest = self._load_manifest(backup_name)
                    backups.append(
                        {
                            "name": backup_name,
                            "type": "full",
                            "created_at": manifest.get("created_at", "unknown"),
                        }
                    )
                except ClientError as e:
                    print(f"Error loading manifest for {backup_name}: {e}")
                except Exception as e:
                    print(f"Unexpected error processing backup {backup_name}: {e}")

        except ClientError as e:
            print(f"Error listing full backups: {e}")

        try:
            manifest = self._load_manifest()
            if manifest.get("files"):
                backups.append(
                    {
                        "name": "mirror_backup",
                        "type": "mirror",
                        "created_at": manifest.get("created_at", "unknown"),
                        "source_directory": manifest.get("source_directory", "unknown"),
                    }
                )
        except ClientError as e:
            print(f"Error loading mirror backup manifest: {e}")
        except Exception as e:
            print(f"Unexpected error processing mirror backup: {e}")

        return backups
