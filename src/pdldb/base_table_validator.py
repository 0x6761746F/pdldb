from typing import Dict, Any
import polars as pl
from pydantic import BaseModel, field_validator
from pydantic.config import ConfigDict

TYPE_MAPPINGS = {
    "int8": pl.Int8,
    "int16": pl.Int16,
    "int32": pl.Int32,
    "int64": pl.Int64,
    "uint8": pl.UInt8,
    "uint16": pl.UInt16,
    "uint32": pl.UInt32,
    "uint64": pl.UInt64,
    "float32": pl.Float32,
    "float": pl.Float32,
    "float64": pl.Float64,
    "double": pl.Float64,
    "string": pl.Utf8,
    "utf8": pl.Utf8,
    "bool": pl.Boolean,
    "boolean": pl.Boolean,
    "date": pl.Date,
    "datetime": pl.Datetime,
    "timestamp": pl.Datetime,
    "timestamp[ns]": pl.Datetime("ns"),
    "timestamp[us]": pl.Datetime("us"),
    "timestamp[ms]": pl.Datetime("ms"),
    "Datetime(time_unit='ns')": pl.Datetime("ns"),
    "Datetime(time_unit='us')": pl.Datetime("us"),
    "Datetime(time_unit='ms')": pl.Datetime("ms"),
    "decimal": pl.Decimal,
    "binary": pl.Binary,
    "list": pl.List,
    "array": pl.List,
}


class BaseTable(BaseModel):
    name: str
    table_schema: Dict[str, Any]
    primary_keys: str

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("primary_keys")
    @classmethod
    def validate_primary_keys(cls, v: str, info) -> str:
        if "table_schema" in info.data:
            pk_columns = [col.strip() for col in v.split(",")]
            for pk_col in pk_columns:
                if pk_col not in info.data["table_schema"]:
                    raise ValueError(
                        f"Primary key column '{pk_col}' not found in schema"
                    )
        return v

    def _check_column_exists(self, col_name: str, df_schema: Dict) -> bool:
        if col_name not in df_schema:
            print(f"Missing column: {col_name}")
            return False
        return True

    def _validate_type(
        self, col_name: str, df_type: pl.DataType, expected_type: str
    ) -> bool:
        expected_type_str = str(expected_type).lower()
        actual_type_str = str(df_type).lower()

        if "datetime" in expected_type_str or "timestamp" in expected_type_str:
            if not isinstance(df_type, pl.Datetime):
                print(f"Column {col_name}: expected datetime/timestamp, got {df_type}")
                return False
            return True

        if expected_type_str == "decimal":
            if not isinstance(df_type, pl.Decimal):
                print(f"Column {col_name}: expected decimal, got {df_type}")
                return False
            return True

        mapped_type = TYPE_MAPPINGS.get(expected_type_str)
        if mapped_type:
            if not isinstance(df_type, mapped_type):
                print(f"Column {col_name}: expected {mapped_type}, got {df_type}")
                return False
            return True

        print(
            f"Column {col_name}: unknown type {expected_type} (actual type: {actual_type_str})"
        )
        return False

    def validate_schema(self, df: pl.DataFrame) -> bool:
        df_schema = df.schema

        for col_name, expected_type in self.table_schema.items():
            if not self._check_column_exists(col_name, df_schema):
                return False

            if not self._validate_type(col_name, df_schema[col_name], expected_type):
                return False

        return True
