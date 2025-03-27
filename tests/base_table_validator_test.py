import pytest
import polars as pl
from pdldb.base_table_validator import BaseTable
from pydantic import ValidationError


@pytest.fixture
def simple_schema():
    return {"id": "int32", "name": "string"}


@pytest.fixture
def valid_table(simple_schema):
    return BaseTable(
        name="test_table",
        table_schema=simple_schema,
        primary_keys="id",
    )


@pytest.fixture
def valid_dataframe():
    return pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]}).with_columns(
        pl.col("id").cast(pl.Int32)
    )


def test_valid_initialization(simple_schema, valid_table):
    assert valid_table.name == "test_table"
    assert valid_table.table_schema == simple_schema
    assert valid_table.primary_keys == "id"


def test_invalid_primary_keys():
    with pytest.raises(ValidationError) as exc_info:
        BaseTable(name="test_table", table_schema={"name": "string"}, primary_keys="id")
    assert "Primary key column 'id' not found in schema" in str(exc_info.value)


def test_missing_required_fields():
    with pytest.raises(ValidationError):
        BaseTable(name="test_table")


def test_invalid_name_type(simple_schema):
    with pytest.raises(ValidationError):
        BaseTable(
            name=123,
            table_schema=simple_schema,
            primary_keys="id",
        )


def test_invalid_schema_type():
    with pytest.raises(ValidationError):
        BaseTable(name="test_table", table_schema="not_a_dict", primary_keys="id")


def test_validate_schema_valid(valid_table, valid_dataframe):
    assert valid_table.validate_schema(valid_dataframe) is True


def test_validate_schema_missing_column(valid_table):
    df = pl.DataFrame({"id": [1, 2, 3]})
    assert valid_table.validate_schema(df) is False


def test_validate_schema_wrong_type(valid_table):
    df = pl.DataFrame({"id": [1, 2, 3], "name": [1.0, 2.0, 3.0]})
    assert valid_table.validate_schema(df) is False


@pytest.fixture
def timestamp_table():
    return BaseTable(
        name="test_table",
        table_schema={"id": "int32", "timestamp": "timestamp[ns]"},
        primary_keys="id",
    )


def test_validate_schema_timestamp(timestamp_table):
    df = pl.DataFrame(
        {"id": [1, 2, 3], "timestamp": ["2023-01-01", "2023-01-02", "2023-01-03"]}
    ).with_columns(
        [
            pl.col("id").cast(pl.Int32),
            pl.col("timestamp").str.strptime(pl.Datetime("ns"), format="%Y-%m-%d"),
        ]
    )
    assert timestamp_table.validate_schema(df) is True


@pytest.fixture
def decimal_table():
    return BaseTable(
        name="test_table",
        table_schema={"id": "int32", "amount": "decimal"},
        primary_keys="id",
    )


def test_validate_schema_decimal(decimal_table):
    df = pl.DataFrame(
        {"id": [1, 2, 3], "amount": ["1.23", "4.56", "7.89"]}
    ).with_columns([pl.col("id").cast(pl.Int32), pl.col("amount").cast(pl.Decimal)])
    assert decimal_table.validate_schema(df) is True
