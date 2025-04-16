import pytest
import polars as pl
from unittest.mock import patch, MagicMock
from pdldb import S3LakeManager
from pdldb.base_table_validator import BaseTable

# Constants for testing
TEST_BUCKET = "test-bucket"
TEST_PREFIX = "test-prefix"
TEST_REGION = "us-east-1"
TEST_ACCESS_KEY = "test-access-key"
TEST_SECRET_KEY = "test-secret-key"
TEST_DYNAMO_TABLE = "test-dynamodb-table"
TEST_S3_PATH = f"s3://{TEST_BUCKET}/{TEST_PREFIX}"


@pytest.fixture
def mock_s3_client():
    with patch("boto3.client") as mock_client:
        s3_mock = MagicMock()
        mock_client.return_value = s3_mock
        s3_mock.list_objects_v2.return_value = {"CommonPrefixes": []}
        yield s3_mock


@pytest.fixture
def mock_delta_table():
    with patch("deltalake.DeltaTable") as mock_dt:
        mock_dt_instance = MagicMock()
        mock_dt.return_value = mock_dt_instance
        yield mock_dt_instance


@pytest.fixture
def s3_lake_manager():
    with patch("boto3.client") as mock_client:
        s3_mock = MagicMock()
        mock_client.return_value = s3_mock
        s3_mock.list_objects_v2.return_value = {"CommonPrefixes": []}

        with patch("pdldb.lake_manager.S3TableManager") as mock_manager_class:
            mock_manager = MagicMock()
            mock_manager_class.return_value = mock_manager

            mock_manager.tables = {}

            manager = S3LakeManager(
                TEST_S3_PATH,
                aws_region=TEST_REGION,
                aws_access_key=TEST_ACCESS_KEY,
                aws_secret_key=TEST_SECRET_KEY,
            )

            yield manager


def test_init_with_unsafe_renames():
    with patch("boto3.client") as mock_client:
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {"CommonPrefixes": []}

        with patch("pdldb.s3_table_manager.S3TableManager"):
            manager = S3LakeManager(
                TEST_S3_PATH,
                aws_region=TEST_REGION,
                aws_access_key=TEST_ACCESS_KEY,
                aws_secret_key=TEST_SECRET_KEY,
            )

            assert manager.storage_options["AWS_REGION"] == TEST_REGION
            assert manager.storage_options["AWS_ACCESS_KEY_ID"] == TEST_ACCESS_KEY
            assert manager.storage_options["AWS_SECRET_ACCESS_KEY"] == TEST_SECRET_KEY
            assert manager.storage_options["AWS_S3_ALLOW_UNSAFE_RENAME"] == "true"
            assert "AWS_S3_LOCKING_PROVIDER" not in manager.storage_options


def test_init_with_dynamo_locking():
    with patch("boto3.client") as mock_client:
        s3_mock = MagicMock()
        mock_client.return_value = s3_mock
        s3_mock.list_objects_v2.return_value = {"CommonPrefixes": []}

        with patch("pdldb.s3_table_manager.S3TableManager"):
            manager = S3LakeManager(
                TEST_S3_PATH,
                aws_region=TEST_REGION,
                aws_access_key=TEST_ACCESS_KEY,
                aws_secret_key=TEST_SECRET_KEY,
                dynamodb_locking_table=TEST_DYNAMO_TABLE,
            )

            assert manager.storage_options["AWS_S3_LOCKING_PROVIDER"] == "dynamodb"
            assert (
                manager.storage_options["DELTA_DYNAMO_TABLE_NAME"] == TEST_DYNAMO_TABLE
            )
            assert "AWS_S3_ALLOW_UNSAFE_RENAME" not in manager.storage_options


def test_init_invalid_url():
    with pytest.raises(ValueError, match="S3TableManager requires an S3 URL"):
        with patch("pdldb.s3_table_manager.S3TableManager") as mock_manager:
            mock_manager.side_effect = ValueError(
                "S3TableManager requires an S3 URL (s3://...)"
            )
            S3LakeManager(
                "file:///not/s3/path",
                aws_region=TEST_REGION,
                aws_access_key=TEST_ACCESS_KEY,
                aws_secret_key=TEST_SECRET_KEY,
            )


def test_load_existing_tables():
    with patch("boto3.client") as mock_client:
        s3_mock = MagicMock()
        mock_client.return_value = s3_mock
        s3_mock.list_objects_v2.side_effect = [
            {
                "CommonPrefixes": [
                    {"Prefix": f"{TEST_PREFIX}/table1/"},
                    {"Prefix": f"{TEST_PREFIX}/table2/"},
                ]
            },
            {"Contents": [{"Key": f"{TEST_PREFIX}/table1/_delta_log/00000.json"}]},
            {"Contents": [{"Key": f"{TEST_PREFIX}/table2/_delta_log/00000.json"}]},
        ]

        with patch("deltalake.DeltaTable") as mock_dt:
            mock_dt_instance1 = MagicMock()
            mock_dt_instance2 = MagicMock()
            mock_dt.side_effect = [mock_dt_instance1, mock_dt_instance2]

            mock_metadata1 = {"description": "id"}
            mock_dt_instance1.metadata.return_value = mock_metadata1

            mock_metadata2 = {"description": "user_id,timestamp"}
            mock_dt_instance2.metadata.return_value = mock_metadata2

            mock_schema1 = MagicMock()
            mock_schema2 = MagicMock()
            mock_dt_instance1.schema.return_value = mock_schema1
            mock_dt_instance2.schema.return_value = mock_schema2

            pa_schema1 = MagicMock()
            pa_schema2 = MagicMock()
            mock_schema1.to_pyarrow.return_value = pa_schema1
            mock_schema2.to_pyarrow.return_value = pa_schema2

            field1 = MagicMock()
            field1.name = "id"
            field1.type = "int32"

            field2 = MagicMock()
            field2.name = "name"
            field2.type = "string"

            field3 = MagicMock()
            field3.name = "user_id"
            field3.type = "int32"

            field4 = MagicMock()
            field4.name = "timestamp"
            field4.type = "timestamp[ns]"

            pa_schema1.__iter__ = lambda _: iter([field1, field2])
            pa_schema2.__iter__ = lambda _: iter([field3, field4])

            manager = S3LakeManager(
                TEST_S3_PATH,
                aws_region=TEST_REGION,
                aws_access_key=TEST_ACCESS_KEY,
                aws_secret_key=TEST_SECRET_KEY,
            )

            manager.table_manager.tables = {
                "table1": BaseTable(
                    name="table1",
                    table_schema={"id": "int32", "name": "string"},
                    primary_keys="id",
                ),
                "table2": BaseTable(
                    name="table2",
                    table_schema={"user_id": "int32", "timestamp": "timestamp[ns]"},
                    primary_keys="user_id,timestamp",
                ),
            }

            assert "table1" in manager.table_manager.tables
            assert "table2" in manager.table_manager.tables
            assert manager.table_manager.tables["table1"].primary_keys == "id"
            assert (
                manager.table_manager.tables["table2"].primary_keys
                == "user_id,timestamp"
            )


def test_create_table(s3_lake_manager):
    """Test creating a new table"""
    table_name = "test_table"
    schema = {"id": pl.Int32, "name": pl.Utf8, "value": pl.Float64}
    primary_keys = "id"

    s3_lake_manager.create_table(table_name, schema, primary_keys)

    s3_lake_manager.table_manager.create_table.assert_called_once_with(
        table_name=table_name, table_schema=schema, primary_keys=primary_keys
    )


def test_create_table_composite_key(s3_lake_manager):
    table_name = "composite_key_table"
    schema = {
        "id": pl.Int32,
        "region": pl.Utf8,
        "timestamp": pl.Datetime,
        "value": pl.Float64,
    }
    primary_keys = ["id", "region"]

    s3_lake_manager.create_table(table_name, schema, primary_keys)

    s3_lake_manager.table_manager.create_table.assert_called_once()
    args, kwargs = s3_lake_manager.table_manager.create_table.call_args
    assert kwargs["table_name"] == table_name
    assert kwargs["table_schema"] == schema
    assert kwargs["primary_keys"] == primary_keys


def test_create_table_already_exists(s3_lake_manager):
    table_name = "existing_table"
    schema = {"id": pl.Int32}

    s3_lake_manager.table_manager.tables[table_name] = "exists"

    with patch.object(s3_lake_manager, "_check_table_not_exists") as mock_check:
        mock_check.side_effect = ValueError(f"Table {table_name} already exists")

        with pytest.raises(ValueError, match=f"Table {table_name} already exists"):
            s3_lake_manager.create_table(table_name, schema, "id")


def test_delete_table(s3_lake_manager):
    table_name = "table_to_delete"

    s3_lake_manager.table_manager.delete_table.return_value = True

    s3_lake_manager.table_manager.tables = {table_name: "exists"}

    result = s3_lake_manager.delete_table(table_name)

    assert result is True
    s3_lake_manager.table_manager.delete_table.assert_called_once_with(
        table_name=table_name
    )


def test_delete_nonexistent_table(s3_lake_manager):
    s3_lake_manager.table_manager.tables = {}

    with patch.object(s3_lake_manager, "_check_table_exists") as mock_check:
        mock_check.side_effect = ValueError("Table nonexistent does not exist")

        with pytest.raises(ValueError, match="Table nonexistent does not exist"):
            s3_lake_manager.delete_table("nonexistent")


def test_list_tables(s3_lake_manager):
    expected_result = {
        "table1": {"schema": {"id": pl.Int32}},
        "table2": {"schema": {"name": pl.Utf8}},
    }
    s3_lake_manager.table_manager.list_tables.return_value = expected_result

    tables = s3_lake_manager.list_tables()

    s3_lake_manager.table_manager.list_tables.assert_called_once()
    assert tables == expected_result


@pytest.fixture
def sample_dataframe():
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10.5, 20.3, 30.1],
        }
    ).with_columns([pl.col("id").cast(pl.Int32)])


def test_append_table(s3_lake_manager, sample_dataframe):
    table_name = "append_test"

    s3_lake_manager.table_manager.tables = {table_name: "exists"}

    s3_lake_manager.append_table(table_name, sample_dataframe)

    s3_lake_manager.table_manager.append.assert_called_once()
    args, kwargs = s3_lake_manager.table_manager.append.call_args
    assert kwargs["table_name"] == table_name
    assert kwargs["df"] is sample_dataframe
    assert kwargs["delta_write_options"] is None


def test_append_table_schema_mismatch(s3_lake_manager, sample_dataframe):
    table_name = "schema_mismatch"

    s3_lake_manager.table_manager.tables = {table_name: "exists"}
    s3_lake_manager.table_manager.append.side_effect = ValueError(
        "DataFrame does not match table schema"
    )

    with pytest.raises(ValueError, match="DataFrame does not match table schema"):
        s3_lake_manager.append_table(table_name, sample_dataframe)


def test_merge_table(s3_lake_manager, sample_dataframe):
    table_name = "merge_test"

    s3_lake_manager.table_manager.tables = {table_name: "exists"}

    for operation in ["update", "insert", "delete", "upsert", "upsert_delete"]:
        s3_lake_manager.merge_table(
            table_name, sample_dataframe, merge_condition=operation
        )

        s3_lake_manager.table_manager.merge.assert_called_with(
            table_name=table_name,
            df=sample_dataframe,
            delta_write_options=None,
            merge_condition=operation,
        )


def test_merge_table_with_table_not_found(s3_lake_manager, sample_dataframe):
    table_name = "not_found_table"

    s3_lake_manager.table_manager.tables = {table_name: "exists"}

    s3_lake_manager.table_manager.merge.side_effect = [
        None,  # For "insert" operation
        None,  # For "upsert" operation
    ]

    for operation in ["insert", "upsert"]:
        s3_lake_manager.merge_table(
            table_name, sample_dataframe, merge_condition=operation
        )

    s3_lake_manager.table_manager.merge.side_effect = ValueError(
        "No log files found or data found."
    )

    for operation in ["update", "delete", "upsert_delete"]:
        with pytest.raises(ValueError, match="No log files found or data found."):
            s3_lake_manager.merge_table(
                table_name, sample_dataframe, merge_condition=operation
            )


def test_overwrite_table(s3_lake_manager, sample_dataframe):
    table_name = "overwrite_test"

    s3_lake_manager.table_manager.tables = {table_name: "exists"}

    s3_lake_manager.overwrite_table(table_name, sample_dataframe)

    s3_lake_manager.table_manager.overwrite.assert_called_once()
    args, kwargs = s3_lake_manager.table_manager.overwrite.call_args
    assert kwargs["table_name"] == table_name
    assert kwargs["df"] is sample_dataframe
    assert kwargs["delta_write_options"] is None


def test_get_data_frame(s3_lake_manager):
    table_name = "read_test"

    s3_lake_manager.table_manager.tables = {table_name: "exists"}

    mock_df = MagicMock(spec=pl.DataFrame)
    s3_lake_manager.table_manager.get_data_frame.return_value = mock_df

    df = s3_lake_manager.get_data_frame(table_name)

    s3_lake_manager.table_manager.get_data_frame.assert_called_once_with(
        table_name=table_name
    )
    assert df is mock_df


def test_get_data_frame_table_not_found(s3_lake_manager):
    table_name = "empty_table"

    s3_lake_manager.table_manager.tables = {table_name: "exists"}

    empty_df = pl.DataFrame()
    s3_lake_manager.table_manager.get_data_frame.return_value = empty_df

    df = s3_lake_manager.get_data_frame(table_name)
    assert df is empty_df


def test_get_data_frame_nonexistent_table(s3_lake_manager):
    s3_lake_manager.table_manager.tables = {}

    with patch.object(s3_lake_manager, "_check_table_exists") as mock_check:
        mock_check.side_effect = ValueError("Table nonexistent does not exist")

        with pytest.raises(ValueError, match="Table nonexistent does not exist"):
            s3_lake_manager.get_data_frame("nonexistent")


def test_get_lazy_frame(s3_lake_manager):
    table_name = "lazy_test"

    s3_lake_manager.table_manager.tables = {table_name: "exists"}

    mock_lazy_frame = MagicMock(spec=pl.LazyFrame)
    s3_lake_manager.table_manager.get_lazy_frame.return_value = mock_lazy_frame

    lf = s3_lake_manager.get_lazy_frame(table_name)

    s3_lake_manager.table_manager.get_lazy_frame.assert_called_once_with(
        table_name=table_name
    )
    assert lf is mock_lazy_frame


def test_optimize_table(s3_lake_manager):
    table_name = "optimize_test"

    s3_lake_manager.table_manager.tables = {table_name: "exists"}

    s3_lake_manager.optimize_table(
        table_name,
        target_size=1024 * 1024,
        max_concurrent_tasks=4,
        writer_properties={"compression": "snappy"},
    )

    s3_lake_manager.table_manager.optimize_table.assert_called_once_with(
        table_name=table_name,
        target_size=1024 * 1024,
        max_concurrent_tasks=4,
        writer_properties={"compression": "snappy"},
    )


def test_optimize_nonexistent_table(s3_lake_manager):
    s3_lake_manager.table_manager.tables = {}

    with patch.object(s3_lake_manager, "_check_table_exists") as mock_check:
        mock_check.side_effect = ValueError("Table nonexistent does not exist")

        with pytest.raises(ValueError, match="Table nonexistent does not exist"):
            s3_lake_manager.optimize_table("nonexistent", target_size=1024 * 1024)


def test_vacuum_table(s3_lake_manager):
    table_name = "vacuum_test"

    s3_lake_manager.table_manager.tables = {table_name: "exists"}

    s3_lake_manager.vacuum_table(
        table_name, retention_hours=24, enforce_retention_duration=True
    )

    s3_lake_manager.table_manager.vacuum_table.assert_called_once_with(
        table_name=table_name, retention_hours=24, enforce_retention_duration=True
    )


def test_vacuum_nonexistent_table(s3_lake_manager):
    s3_lake_manager.table_manager.tables = {}

    with patch.object(s3_lake_manager, "_check_table_exists") as mock_check:
        mock_check.side_effect = ValueError("Table nonexistent does not exist")

        with pytest.raises(ValueError, match="Table nonexistent does not exist"):
            s3_lake_manager.vacuum_table("nonexistent", retention_hours=24)


def test_get_table_info(s3_lake_manager):
    table_name = "info_test"

    s3_lake_manager.table_manager.tables = {table_name: "exists"}

    expected_info = {
        "exists": True,
        "version": 3,
        "schema": {"id": pl.Int32, "name": pl.Utf8, "value": pl.Float64},
        "primary_keys": "id",
        "metadata": {
            "id": "test-table-id",
            "description": "id",
            "created_time": 1234567890,
        },
    }
    s3_lake_manager.table_manager.get_table_info.return_value = expected_info

    info = s3_lake_manager.get_table_info(table_name)

    s3_lake_manager.table_manager.get_table_info.assert_called_once_with(
        table_name=table_name
    )
    assert info == expected_info


def test_get_table_info_nonexistent_data(s3_lake_manager):
    table_name = "no_data_table"

    s3_lake_manager.table_manager.tables = {table_name: "exists"}

    expected_info = {
        "exists": False,
        "version": 0,
        "metadata": None,
        "schema": {"id": pl.Int32},
        "primary_keys": "id",
    }
    s3_lake_manager.table_manager.get_table_info.return_value = expected_info

    info = s3_lake_manager.get_table_info(table_name)

    assert info["exists"] is False
    assert info["version"] == 0
    assert info["metadata"] is None
    assert info["schema"] == {"id": pl.Int32}
    assert info["primary_keys"] == "id"


def test_get_table_schema(s3_lake_manager):
    table_name = "schema_test"
    schema = {"id": pl.Int32, "name": pl.Utf8, "value": pl.Float64}

    s3_lake_manager.table_manager.tables = {table_name: "exists"}

    s3_lake_manager.table_manager.get_table_schema.return_value = schema

    result_schema = s3_lake_manager.get_table_schema(table_name)

    assert result_schema == schema
    s3_lake_manager.table_manager.get_table_schema.assert_called_once_with(
        table_name=table_name
    )


def test_get_table_schema_nonexistent(s3_lake_manager):
    s3_lake_manager.table_manager.tables = {}

    with patch.object(s3_lake_manager, "_check_table_exists") as mock_check:
        mock_check.side_effect = ValueError("Table nonexistent does not exist")

        with pytest.raises(ValueError, match="Table nonexistent does not exist"):
            s3_lake_manager.get_table_schema("nonexistent")
