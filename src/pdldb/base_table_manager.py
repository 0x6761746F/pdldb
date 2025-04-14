from typing import Optional, List, Dict, Any, Union
import polars as pl
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from pdldb.base_table_validator import BaseTable
from abc import ABC, abstractmethod
from pathlib import Path


class BaseTableManager(ABC):
    def __init__(
        self, delta_table_path: str, storage_options: Optional[Dict[str, str]] = None
    ):
        self.storage_options = storage_options
        self.base_path = delta_table_path
        self.tables: Dict[str, BaseTable] = {}

    def create_table(
        self,
        table_name: str,
        table_schema: Dict[str, Any],
        primary_keys: Union[str, List[str]],
    ) -> None:
        if isinstance(primary_keys, list):
            primary_keys = ",".join(primary_keys)

        base_table = BaseTable(
            name=table_name, table_schema=table_schema, primary_keys=primary_keys
        )
        self.tables[table_name] = base_table

    def _get_full_path(self, table_name: str) -> str:
        if isinstance(self.base_path, Path):
            return str(self.base_path / table_name)
        else:
            return f"{self.base_path}{table_name}"

    def append(
        self,
        table_name: str,
        df: pl.DataFrame,
        delta_write_options: Optional[Dict[str, Any]],
    ) -> None:
        base_table = self.tables[table_name]
        if not base_table.validate_schema(df):
            raise ValueError("DataFrame does not match table schema")
        delta_write_options = delta_write_options or {}
        delta_write_options["description"] = base_table.primary_keys

        df.write_delta(
            self._get_full_path(table_name),
            mode="append",
            delta_write_options=delta_write_options,
            storage_options=self.storage_options,
        )

    def merge(
        self,
        table_name: str,
        df: pl.DataFrame,
        merge_condition: str,
        delta_write_options: Optional[Dict[str, Any]],
    ) -> None:
        base_table = self.tables[table_name]
        if not base_table.validate_schema(df):
            raise ValueError("DataFrame does not match table schema")

        primary_keys = base_table.primary_keys
        delta_write_options = delta_write_options or {}
        delta_write_options["description"] = primary_keys
        mode = "merge"

        if "," in primary_keys:
            pk_columns = [col.strip() for col in primary_keys.split(",")]
            predicate_conditions = [f"s.{col} = t.{col}" for col in pk_columns]
            predicate = " AND ".join(predicate_conditions)
        else:
            predicate = f"s.{primary_keys} = t.{primary_keys}"

        delta_merge_options = {
            "predicate": predicate,
            "source_alias": "s",
            "target_alias": "t",
        }

        try:
            df = df.write_delta(
                self._get_full_path(table_name),
                mode=mode,
                delta_write_options=delta_write_options,
                storage_options=self.storage_options,
                delta_merge_options=delta_merge_options,
            )

            if merge_condition == "update":
                df.when_matched_update_all().execute()

            if merge_condition == "insert":
                df.when_not_matched_insert_all().execute()

            if merge_condition == "delete":
                df.when_matched_delete().execute()

            if merge_condition == "upsert":
                df.when_matched_update_all().when_not_matched_insert_all().execute()

            if merge_condition == "upsert_delete":
                df.when_matched_update_all().when_not_matched_insert_all().when_not_matched_by_source_delete().execute()

        except Exception as e:
            if isinstance(e, TableNotFoundError):
                if merge_condition in ["insert", "upsert"]:
                    df.write_delta(
                        self._get_full_path(table_name),
                        mode="append",
                        delta_write_options=delta_write_options,
                        storage_options=self.storage_options,
                    )
                else:
                    raise ValueError(
                        f"No log files found or data found. Please check if table data exists or if the merge condition ({merge_condition}) is correct."
                    )
            else:
                raise ValueError(f"An error occurred during the merge operation: {e}")

    def overwrite(
        self,
        table_name: str,
        df: pl.DataFrame,
        delta_write_options: Optional[Dict[str, Any]],
    ) -> None:
        base_table = self.tables[table_name]
        if not base_table.validate_schema(df):
            raise ValueError("DataFrame does not match table schema")

        delta_write_options = delta_write_options or {}
        delta_write_options["description"] = base_table.primary_keys

        df.write_delta(
            self._get_full_path(table_name),
            mode="overwrite",
            delta_write_options=delta_write_options,
            storage_options=self.storage_options,
        )

    def get_data_frame(self, table_name: str) -> pl.DataFrame:
        try:
            return pl.read_delta(
                self._get_full_path(table_name), storage_options=self.storage_options
            )
        except (TableNotFoundError, FileNotFoundError) as e:
            if table_name in self.tables:
                schema = self.tables[table_name].table_schema
                return pl.DataFrame(schema=schema)
            else:
                raise ValueError(
                    f"Table '{table_name}' does not exist or has no data: {e}"
                )

    def get_lazy_frame(self, table_name: str) -> pl.LazyFrame:
        try:
            return pl.scan_delta(
                self._get_full_path(table_name), storage_options=self.storage_options
            )
        except (TableNotFoundError, FileNotFoundError) as e:
            if table_name in self.tables:
                schema = self.tables[table_name].table_schema
                return pl.DataFrame(schema=schema).lazy()
            else:
                raise ValueError(
                    f"Table '{table_name}' does not exist or has no data: {e}"
                )

    def optimize_table(
        self,
        table_name: str,
        target_size: Optional[int],
        max_concurrent_tasks: Optional[int],
        writer_properties: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        try:
            delta_table = DeltaTable(
                self._get_full_path(table_name), storage_options=self.storage_options
            )

            str(
                delta_table.optimize.compact(
                    target_size=target_size,
                    max_concurrent_tasks=max_concurrent_tasks,
                    writer_properties=writer_properties,
                )
            )
        except (TableNotFoundError, FileNotFoundError) as e:
            raise ValueError(f"Table '{table_name}' does not exist or has no data: {e}")

    def vacuum_table(
        self,
        table_name: str,
        retention_hours: Optional[int],
        enforce_retention_duration: bool,
    ) -> List[str]:
        try:
            delta_table = DeltaTable(
                self._get_full_path(table_name), storage_options=self.storage_options
            )
            str(
                delta_table.vacuum(
                    retention_hours=retention_hours,
                    dry_run=False,
                    enforce_retention_duration=enforce_retention_duration,
                )
            )
        except (TableNotFoundError, FileNotFoundError) as e:
            raise ValueError(f"Table '{table_name}' does not exist or has no data: {e}")

    def list_tables(self) -> Dict[str, Dict[str, Any]]:
        return {name: self.get_table_info(name) for name in self.tables.keys()}

    def get_table_info(self, table_name: str) -> dict:
        table_path = self._get_full_path(table_name)
        base_table = self.tables[table_name]

        table_info = {
            "exists": False,
            "version": 0,
            "metadata": None,
            "size": 0,
            "schema": base_table.table_schema,
            "primary_keys": base_table.primary_keys,
        }

        try:
            dt = DeltaTable(table_path, storage_options=self.storage_options)

            size = 0
            path_obj = Path(table_path)
            if path_obj.exists():
                size = len(list(path_obj.glob("*.parquet")))

            return {
                "exists": True,
                "version": dt.version(),
                "metadata": dt.metadata(),
                "size": size,
                "schema": base_table.table_schema,
                "primary_keys": base_table.primary_keys,
            }
        except (TableNotFoundError, FileNotFoundError):
            return table_info

    def get_table_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        return self.tables[table_name].table_schema

    @abstractmethod
    def delete_table(self, table_name: str) -> bool:
        pass
