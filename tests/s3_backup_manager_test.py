import os
import json
import tempfile
import boto3
import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from pdldb.s3_backup_manager import S3BackupManager


@pytest.fixture
def mock_s3_client():
    with patch("boto3.client") as mock_client:
        s3_mock = MagicMock()
        mock_client.return_value = s3_mock
        yield s3_mock


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def test_files(temp_dir):
    test_dir = os.path.join(temp_dir, "test_data")
    os.makedirs(test_dir, exist_ok=True)

    with open(os.path.join(test_dir, "file1.txt"), "w") as f:
        f.write("test content 1")

    with open(os.path.join(test_dir, "file2.txt"), "w") as f:
        f.write("test content 2")

    subdir = os.path.join(test_dir, "subdir")
    os.makedirs(subdir, exist_ok=True)
    with open(os.path.join(subdir, "file3.txt"), "w") as f:
        f.write("test content 3")

    return test_dir


class TestS3BackupManager:
    def test_init_with_params(self, mock_s3_client):
        manager = S3BackupManager(
            bucket_name="test-bucket",
            aws_region="us-west-2",
            prefix="test-prefix/",
            endpoint_url="http://localhost:4566",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
        )

        assert manager.bucket == "test-bucket"
        assert manager.prefix == "test-prefix/"
        assert manager.full_prefix == "test-prefix/full_backups/"
        assert manager.mir_prefix == "test-prefix/mirror_backup/"

        boto3.client.assert_called_once_with(
            "s3",
            region_name="us-west-2",
            endpoint_url="http://localhost:4566",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
        )

    def test_init_with_env_vars(self, mock_s3_client):
        with patch.dict(
            os.environ,
            {
                "AWS_REGION": "eu-west-1",
                "AWS_ACCESS_KEY_ID": "env-key",
                "AWS_SECRET_ACCESS_KEY": "env-secret",
                "S3_BUCKET_NAME": "env-bucket",
            },
        ):
            manager = S3BackupManager()

            assert manager.bucket == "env-bucket"
            assert manager.prefix == "pdldb_backups/"

            boto3.client.assert_called_once_with(
                "s3",
                region_name="eu-west-1",
                endpoint_url=None,
                aws_access_key_id="env-key",
                aws_secret_access_key="env-secret",
            )

    def test_init_no_bucket(self, mock_s3_client):
        with patch.dict(os.environ, {"S3_BUCKET_NAME": ""}, clear=True):
            with pytest.raises(ValueError, match="S3 bucket name is required"):
                S3BackupManager()

    @patch("tempfile.NamedTemporaryFile")
    @patch("tarfile.open")
    @patch("os.path.getmtime", return_value=1234567890.0)
    def test_full_backup(
        self, mock_getmtime, mock_tarfile, mock_tempfile, test_files, mock_s3_client
    ):
        mock_tar = MagicMock()
        mock_tarfile.return_value.__enter__.return_value = mock_tar

        temp_file = MagicMock()
        temp_file.name = "/tmp/tempfile.tar.gz"
        mock_tempfile.return_value.__enter__.return_value = temp_file

        mock_s3_client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}}, "GetObject"
        )

        with patch.object(
            S3BackupManager, "_get_file_hash", return_value="fakehash123"
        ):
            manager = S3BackupManager(bucket_name="test-bucket")

            backup_name = manager.full_backup(test_files)

            mock_tarfile.assert_called_with("/tmp/tempfile.tar.gz", "w:gz")
            mock_tar.add.assert_called_with(
                test_files, arcname=os.path.basename(test_files)
            )

            mock_s3_client.upload_file.assert_called_with(
                "/tmp/tempfile.tar.gz",
                "test-bucket",
                f"{manager.full_prefix}{backup_name}/full_backup.tar.gz",
                ExtraArgs={"StorageClass": "STANDARD"},
            )

            assert mock_s3_client.put_object.call_count == 1
            args, kwargs = mock_s3_client.put_object.call_args
            assert kwargs["Bucket"] == "test-bucket"
            assert kwargs["Key"].endswith("manifest.json")

            manifest_data = json.loads(kwargs["Body"].decode("utf-8"))
            assert "files" in manifest_data
            assert "created_at" in manifest_data
            assert manifest_data["type"] == "full"

    @patch("os.path.getmtime", return_value=1234567890.0)
    def test_mirror_backup(self, mock_getmtime, test_files, mock_s3_client):
        mock_s3_client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}}, "GetObject"
        )

        paginator_mock = MagicMock()
        mock_pages = MagicMock()
        mock_pages.paginate.return_value = [{"Contents": []}]
        paginator_mock.paginate.return_value = [{"Contents": []}]
        mock_s3_client.get_paginator.return_value = paginator_mock

        with patch.object(
            S3BackupManager, "_get_file_hash", return_value="fakehash123"
        ):
            manager = S3BackupManager(bucket_name="test-bucket")

            result = manager.mirror_backup(test_files)

            assert result == "mirror_backup"

            assert mock_s3_client.upload_file.call_count == 3

            assert mock_s3_client.put_object.call_count == 1
            args, kwargs = mock_s3_client.put_object.call_args
            assert kwargs["Bucket"] == "test-bucket"
            assert kwargs["Key"] == manager._get_mir_manifest_key()

            manifest_data = json.loads(kwargs["Body"].decode("utf-8"))
            assert "files" in manifest_data
            assert "created_at" in manifest_data
            assert manifest_data["type"] == "mirror"
            assert manifest_data["source_directory"] == os.path.basename(test_files)
            assert len(manifest_data["files"]) == 3

    @patch("tempfile.TemporaryDirectory")
    @patch("tarfile.open")
    def test_restore_full_backup(
        self, mock_tarfile, mock_tempdir, temp_dir, mock_s3_client
    ):
        mock_tempdir.return_value.__enter__.return_value = "/tmp/temp_extract"

        manifest = {
            "files": {
                "file1.txt": {"hash": "hash1", "mtime": 123456},
                "file2.txt": {"hash": "hash2", "mtime": 123457},
                "subdir/file3.txt": {"hash": "hash3", "mtime": 123458},
            },
            "created_at": datetime.now().isoformat(),
            "type": "full",
        }

        mock_s3_client.get_object.return_value = {
            "Body": MagicMock(read=lambda: json.dumps(manifest).encode())
        }

        mock_tar = MagicMock()
        mock_tarfile.return_value.__enter__.return_value = mock_tar

        manager = S3BackupManager(bucket_name="test-bucket")

        result = manager.restore("backup_20230101", temp_dir)

        assert result is True

        mock_s3_client.download_file.assert_called_with(
            "test-bucket",
            f"{manager.full_prefix}backup_20230101/full_backup.tar.gz",
            os.path.join("/tmp/temp_extract", "backup_20230101.tar.gz"),
        )

        mock_tarfile.assert_called_with(
            os.path.join("/tmp/temp_extract", "backup_20230101.tar.gz"), "r:gz"
        )
        mock_tar.extractall.assert_called_with(path=temp_dir)

    def test_restore_mirror_backup(self, temp_dir, mock_s3_client):
        manifest = {
            "files": {
                "file1.txt": {"hash": "hash1", "mtime": 123456},
                "file2.txt": {"hash": "hash2", "mtime": 123457},
                "subdir/file3.txt": {"hash": "hash3", "mtime": 123458},
            },
            "created_at": datetime.now().isoformat(),
            "type": "mirror",
            "source_directory": "test_data",
        }

        mock_s3_client.get_object.return_value = {
            "Body": MagicMock(read=lambda: json.dumps(manifest).encode())
        }

        paginator_mock = MagicMock()
        paginator_mock.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "pdldb_backups/mirror_backup/file1.txt"},
                    {"Key": "pdldb_backups/mirror_backup/file2.txt"},
                    {"Key": "pdldb_backups/mirror_backup/subdir/file3.txt"},
                    {"Key": "pdldb_backups/mirror_backup/manifest.json"},
                ]
            }
        ]
        mock_s3_client.get_paginator.return_value = paginator_mock

        manager = S3BackupManager(bucket_name="test-bucket")

        result = manager.restore("mirror_backup", temp_dir)

        assert result is True

        assert mock_s3_client.download_file.call_count == 3

    def test_list_backups(self, mock_s3_client):
        mock_s3_client.list_objects_v2.return_value = {
            "CommonPrefixes": [
                {"Prefix": "pdldb_backups/full_backups/backup1/"},
                {"Prefix": "pdldb_backups/full_backups/backup2/"},
            ]
        }

        manifest1 = {"files": {}, "created_at": "2023-01-01T12:00:00", "type": "full"}

        manifest2 = {"files": {}, "created_at": "2023-01-02T12:00:00", "type": "full"}

        mirror_manifest = {
            "files": {"file1.txt": {"hash": "hash1", "mtime": 123456}},
            "created_at": "2023-01-03T12:00:00",
            "type": "mirror",
            "source_directory": "test_data",
        }

        def mock_get_object(**kwargs):
            key = kwargs.get("Key")
            if "backup1" in key:
                return {"Body": MagicMock(read=lambda: json.dumps(manifest1).encode())}
            elif "backup2" in key:
                return {"Body": MagicMock(read=lambda: json.dumps(manifest2).encode())}
            else:
                return {
                    "Body": MagicMock(read=lambda: json.dumps(mirror_manifest).encode())
                }

        mock_s3_client.get_object.side_effect = mock_get_object

        manager = S3BackupManager(bucket_name="test-bucket")

        backups = manager.list_backups()

        assert len(backups) == 3

        assert any(b["name"] == "backup1" and b["type"] == "full" for b in backups)
        assert any(b["name"] == "backup2" and b["type"] == "full" for b in backups)

        mirror_backup = next(b for b in backups if b["name"] == "mirror_backup")
        assert mirror_backup["type"] == "mirror"
        assert mirror_backup["source_directory"] == "test_data"
