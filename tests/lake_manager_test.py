import shutil
import tempfile
from pathlib import Path
import polars as pl
import pytest
from pdldb.lake_manager import LakeManager, LocalLakeManager


@pytest.fixture
def temp_dir():
    directory = tempfile.mkdtemp()
    yield directory
    shutil.rmtree(directory)


@pytest.fixture
def sample_data():
    return pl.DataFrame(
        {"id": [1, 2, 3], "name": ["foo", "bar", "baz"], "value": [1.1, 2.2, 3.3]}
    )


@pytest.fixture
def update_data():
    return pl.DataFrame(
        {
            "id": [1, 2, 4],
            "name": ["updated_foo", "updated_bar", "new_entry"],
            "value": [10.1, 20.2, 40.4],
        }
    )


@pytest.fixture
def schema():
    return {"id": pl.Int64, "name": pl.Utf8, "value": pl.Float64}


@pytest.fixture
def lake_manager(temp_dir):
    return LocalLakeManager(base_path=temp_dir)


def test_initialization(temp_dir):
    lake_manager = LakeManager(base_path=temp_dir)
    assert lake_manager.base_path == Path(temp_dir)
    assert lake_manager.storage_options is None

    storage_options = {"key": "value"}
    lake_manager = LakeManager(base_path=temp_dir, storage_options=storage_options)
    assert lake_manager.storage_options == storage_options


def test_initialization_with_empty_path():
    with pytest.raises(ValueError):
        LakeManager(base_path="")


def test_context_manager(temp_dir):
    with LakeManager(base_path=temp_dir) as lake_manager:
        assert isinstance(lake_manager, LakeManager)


def test_create_list_tables(lake_manager, schema, sample_data):
    lake_manager.create_table(
        table_name="test_table", table_schema=schema, primary_keys="id"
    )

    lake_manager.append_table(table_name="test_table", df=sample_data)

    tables = lake_manager.list_tables()
    assert "test_table" in tables

    with pytest.raises(ValueError):
        lake_manager.create_table(
            table_name="test_table", table_schema=schema, primary_keys="id"
        )


def test_table_operations(lake_manager, schema, sample_data):
    lake_manager.create_table(
        table_name="operations_table", table_schema=schema, primary_keys="id"
    )

    lake_manager.append_table(table_name="operations_table", df=sample_data)

    result = lake_manager.get_data_frame("operations_table")
    assert len(result) == 3

    lazy_frame = lake_manager.get_lazy_frame("operations_table")
    lazy_result = lazy_frame.collect()
    assert len(lazy_result) == 3

    overwrite_data = pl.DataFrame(
        {"id": [5, 6], "name": ["five", "six"], "value": [5.5, 6.6]}
    )

    lake_manager.overwrite_table(table_name="operations_table", df=overwrite_data)

    result = lake_manager.get_data_frame("operations_table")
    assert len(result) == 2
    assert all(id in [5, 6] for id in result["id"])


def test_merge_operations(lake_manager, schema, sample_data, update_data):
    lake_manager.create_table(
        table_name="merge_table", table_schema=schema, primary_keys="id"
    )

    lake_manager.append_table(table_name="merge_table", df=sample_data)

    lake_manager.merge_table(
        table_name="merge_table", df=update_data, merge_condition="insert"
    )

    result = lake_manager.get_data_frame("merge_table")
    assert len(result) == 4

    lake_manager.merge_table(
        table_name="merge_table", df=update_data, merge_condition="update"
    )

    result = lake_manager.get_data_frame("merge_table")
    updated_record = result.filter(pl.col("id") == 1)
    assert updated_record["name"][0] == "updated_foo"

    additional_data = pl.DataFrame(
        {"id": [7, 2], "name": ["seven", "updated_again"], "value": [7.7, 22.2]}
    )

    lake_manager.merge_table(
        table_name="merge_table", df=additional_data, merge_condition="upsert"
    )

    result = lake_manager.get_data_frame("merge_table")
    assert len(result) == 5
    updated_id2 = result.filter(pl.col("id") == 2)
    assert updated_id2["value"][0] == 22.2


def test_optimize_vacuum(lake_manager, schema, sample_data):
    lake_manager.create_table(
        table_name="maintenance_table", table_schema=schema, primary_keys="id"
    )

    lake_manager.append_table(table_name="maintenance_table", df=sample_data)

    lake_manager.optimize_table(
        table_name="maintenance_table", target_size=1024 * 1024, max_concurrent_tasks=2
    )

    lake_manager.vacuum_table(
        table_name="maintenance_table",
        retention_hours=0,
        enforce_retention_duration=False,
    )

    result = lake_manager.get_data_frame("maintenance_table")
    assert len(result) == len(sample_data)


def test_table_info_and_schema(lake_manager, schema, sample_data):
    lake_manager.create_table(
        table_name="info_table", table_schema=schema, primary_keys="id"
    )

    lake_manager.append_table(table_name="info_table", df=sample_data)

    table_info = lake_manager.get_table_info("info_table")
    assert isinstance(table_info, dict)

    schema_result = lake_manager.get_table_schema("info_table")
    assert isinstance(schema_result, dict)
    assert set(schema_result.keys()) == set(schema.keys())


def test_error_handling(lake_manager, sample_data):
    with pytest.raises(ValueError):
        lake_manager.get_data_frame("non_existent_table")

    with pytest.raises(ValueError):
        lake_manager.append_table(table_name="non_existent_table", df=sample_data)

    with pytest.raises(ValueError):
        lake_manager.overwrite_table(table_name="non_existent_table", df=sample_data)

    with pytest.raises(ValueError):
        lake_manager.merge_table(table_name="non_existent_table", df=sample_data)

    with pytest.raises(ValueError):
        lake_manager.optimize_table(table_name="non_existent_table")

    with pytest.raises(ValueError):
        lake_manager.vacuum_table(table_name="non_existent_table")


def test_delete_table(lake_manager, schema, sample_data):
    lake_manager.create_table(
        table_name="delete_table", table_schema=schema, primary_keys="id"
    )

    lake_manager.append_table(table_name="delete_table", df=sample_data)

    lake_manager.delete_table("delete_table")

    with pytest.raises(ValueError):
        lake_manager.get_data_frame("delete_table")

    with pytest.raises(ValueError):
        lake_manager.delete_table("non_existent_table")
