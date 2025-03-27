from pdldb.base_table_manager import BaseTableManager
from pdldb.base_table_validator import BaseTable
from deltalake import DeltaTable
from typing import Dict, Optional
import shutil


class LocalTableManager(BaseTableManager):
    def __init__(
        self, delta_table_path: str, storage_options: Optional[Dict[str, str]] = None
    ):
        super().__init__(delta_table_path, storage_options)

        self._load_existing_tables()

    def _load_existing_tables(self) -> None:
        if not self.base_path.exists():
            return

        for path in self.base_path.iterdir():
            if not (path.is_dir() and (path / "_delta_log").exists()):
                continue

            table_name = path.name
            dt = DeltaTable(str(path), storage_options=self.storage_options)

            primary_keys = dt.metadata().description or "unknown_primary_keys"

            pa_schema = dt.schema().to_pyarrow()
            schema_dict = {field.name: str(field.type) for field in pa_schema}

            base_table = BaseTable(
                name=table_name,
                table_schema=schema_dict,
                primary_keys=primary_keys,
            )
            self.tables[table_name] = base_table

    def delete_table(self, table_name: str) -> bool:
        try:
            delta_table = DeltaTable(
                str(self.base_path / table_name), storage_options=self.storage_options
            )
            delta_table.delete()

            shutil.rmtree(str(self.base_path / table_name))
            del self.tables[table_name]
            return True
        except Exception as e:
            print(f"Failed to delete table {table_name}: {e}")
            return False
