import os

os.environ["RUST_LOG"] = "error"

from typing import Dict, Any, Optional, List, Union, Literal
import polars as pl
from pathlib import Path
from pdldb.local_table_manager import LocalTableManager
from pdldb.s3_table_manager import S3TableManager
from pydantic import BaseModel, Field, field_validator, ConfigDict


class LakeManagerInitModel(BaseModel):
    base_path: str = Field(..., description="Base path for the lake storage")
    storage_options: Optional[Dict[str, Any]] = Field(
        None, description="Storage options for the lake"
    )

    model_config = ConfigDict(extra="forbid")

    @field_validator("base_path")
    @classmethod
    def validate_base_path(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Base path cannot be empty")
        return v


class TableCreateModel(BaseModel):
    table_name: str = Field(..., description="Name of the table to create")
    table_schema: Dict[str, Any] = Field(
        ..., description="Schema definition for the table"
    )
    primary_keys: Union[str, List[str]] = Field(
        ..., description="Primary key column(s)"
    )

    model_config = ConfigDict(extra="forbid")

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Table name cannot be empty")
        return v


class TableOperationModel(BaseModel):
    table_name: str = Field(..., description="Name of the table")
    df: pl.DataFrame = Field(..., description="Data to write")
    delta_write_options: Optional[Dict[str, Any]] = Field(
        None, description="Options for delta write operation"
    )

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Table name cannot be empty")
        return v


class MergeOperationModel(TableOperationModel):
    merge_condition: Literal[
        "update", "insert", "delete", "upsert", "upsert_delete"
    ] = Field("insert", description="Type of merge operation to perform")


class OptimizeTableModel(BaseModel):
    table_name: str = Field(..., description="Name of the table")
    target_size: int = Field(512 * 1024 * 1024, description="Target file size in bytes")
    max_concurrent_tasks: Optional[int] = Field(
        None, description="Maximum number of concurrent tasks"
    )
    writer_properties: Optional[Dict[str, Any]] = Field(
        None, description="Writer properties"
    )

    model_config = ConfigDict(extra="forbid")


class VacuumTableModel(BaseModel):
    table_name: str = Field(..., description="Name of the table")
    retention_hours: Optional[int] = Field(0, description="Retention hours for files")
    enforce_retention_duration: Optional[bool] = Field(
        False, description="Whether to enforce retention duration"
    )

    model_config = ConfigDict(extra="forbid")


class TableNameModel(BaseModel):
    table_name: str = Field(..., description="Name of the table to operate on")

    model_config = ConfigDict(extra="forbid")

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Table name cannot be empty")
        return v


class LakeManager:
    """
    Base class for managing a data lake with tables stored in Delta format.

    This class provides the foundation for creating, reading, updating, and managing
    Delta tables in a data lake. It's designed to be extended by specific implementations
    like LocalLakeManager.
    """

    def __init__(
        self, base_path: str, storage_options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize a new LakeManager.

        Args:
            base_path: The base path where the data lake will be stored
            storage_options: Optional cloud storage-specific parameters
        """
        params = LakeManagerInitModel(
            base_path=base_path, storage_options=storage_options
        )
        if params.base_path.startswith("s3://"):
            self.base_path = params.base_path
        else:
            self.base_path = Path(params.base_path)

        if isinstance(self.base_path, str):
            if not self.base_path.endswith("/"):
                self.base_path += "/"
        else:
            path_str = str(self.base_path)
            if not path_str.endswith(os.path.sep):
                self.base_path = Path(f"{path_str}{os.path.sep}")

        self.storage_options = params.storage_options
        self.table_manager = None

    def _check_table_exists(self, table_name: str) -> None:
        if table_name not in self.table_manager.tables:
            raise ValueError(f"Table {table_name} does not exist")

    def _check_table_not_exists(self, table_name: str) -> None:
        if table_name in self.table_manager.tables:
            raise ValueError(f"Table {table_name} already exists")

    def create_table(
        self,
        table_name: str,
        table_schema: Dict[str, Any],
        primary_keys: Union[str, List[str]],
    ) -> None:
        """
        Create a new table in the data lake.

        Args:
            table_name: Name of the table to create
            table_schema: Schema definition for the new table
            primary_keys: Primary key column(s) for the table

        Notes:
            - The schema is enforced for write operations.
            - The primary keys are used to identify unique records for merge operations.
            - The primary key can be a single column or a composite key (multiple columns).
            - Primary keys can be specified as a string or a list of strings.

        Example: Single primary key
            ```python
            from pdldb import LocalLakeManager
            import polars as pl

            lake_manager = LocalLakeManager("data")
            schema = {
                "sequence": pl.Int32,
                "value_1": pl.Float64,
                "value_2": pl.Utf8,
                "value_3": pl.Float64,
                "value_4": pl.Float64,
                "value_5": pl.Datetime("ns"),
            }
            primary_keys = "sequence"
            lake_manager.create_table("my_table", schema, primary_keys)
            ```

        Example: Composite primary key
            ```python
            primary_keys = ["sequence", "value_1"]
            lake_manager.create_table("my_table", schema, primary_keys)
            ```
        """
        params = TableCreateModel(
            table_name=table_name, table_schema=table_schema, primary_keys=primary_keys
        )

        self._check_table_not_exists(table_name=params.table_name)
        self.table_manager.create_table(
            table_name=params.table_name,
            table_schema=params.table_schema,
            primary_keys=params.primary_keys,
        )

    def append_table(
        self,
        table_name: str,
        df: pl.DataFrame,
        delta_write_options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Append data to an existing table.

        Args:
            table_name: Name of the table to append to
            df: DataFrame containing the data to append
            delta_write_options: Optional configuration for the delta write operation

        Notes:
            - The schema of the DataFrame must match the schema of the table
            - Appending data to a table has been intialized but contains no data will create the table on your storage backend.

        Example:
            ```python
            lake_manager.append_table("my_table", newdata)
            ```
        """
        params = TableOperationModel(
            table_name=table_name, df=df, delta_write_options=delta_write_options
        )

        self._check_table_exists(table_name=params.table_name)
        self.table_manager.append(
            table_name=params.table_name,
            df=params.df,
            delta_write_options=params.delta_write_options,
        )

    def merge_table(
        self,
        table_name: str,
        df: pl.DataFrame,
        merge_condition: str = "insert",
        delta_write_options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Merge data into an existing table based on the specified merge condition.

        Args:
            table_name: Name of the table to merge data into
            df: DataFrame containing the data to merge
            merge_condition: Type of merge operation to perform (update, insert, delete, upsert, upsert_delete)
            delta_write_options: Optional configuration for the delta write operation

        merge_condition:
            - update: Update existing rows only from the new data
            - insert: Insert new rows only from the new data
            - delete: Delete existing rows that exist in the new data
            - upsert: Update existing rows and insert new rows from the new data
            - upsert_delete: Update existing rows, insert new rows, and delete rows that don't exist in the new data

        Notes:
            - If the table has been intialized but contains no data, merge operations requiring existing data ('update', 'delete', 'upsert_delete') will fail with an error message.
            - The 'insert' and upsert' operations will create the table on your storage backend if the table has been intialized but contains no data.
            - Primary keys defined for the table are used to determine matching records.

        Example:
            ```python
            lake_manager.merge_table("my_table", new_data, merge_condition="upsert")
            ```
        """
        params = MergeOperationModel(
            table_name=table_name,
            df=df,
            merge_condition=merge_condition,
            delta_write_options=delta_write_options,
        )

        self._check_table_exists(table_name=params.table_name)

        self.table_manager.merge(
            table_name=params.table_name,
            df=params.df,
            delta_write_options=params.delta_write_options,
            merge_condition=params.merge_condition,
        )

    def overwrite_table(
        self,
        table_name: str,
        df: pl.DataFrame,
        delta_write_options: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Overwrite an existing table with new data.

        Args:
            table_name: Name of the table to overwrite
            df: DataFrame containing the new data
            delta_write_options: Optional configuration for the delta write operation

        Notes:
            - The schema of the DataFrame must match the schema of the table
            - Overwriting a table that has been intialized but contains no data will create the table on your storage backend.
            - Overwriting a table with existing data will replace the entire table.

        Example:
            ```python
            lake_manager.overwrite_table("my_table", new_data)
            ```
        """
        params = TableOperationModel(
            table_name=table_name, df=df, delta_write_options=delta_write_options
        )

        self._check_table_exists(table_name=params.table_name)
        self.table_manager.overwrite(
            table_name=params.table_name,
            df=params.df,
            delta_write_options=params.delta_write_options,
        )

    def get_data_frame(self, table_name: str) -> pl.DataFrame:
        """
        Get an eager DataFrame from a table.

        Args:
            table_name: Name of the table to read

        Returns:
            A Polars DataFrame containing the table data

        Notes:
            - All table data is loaded into a Polars DataFrame in memory.
            - This is suitable for small to medium-sized tables.

        Example:
            ```python
            df = lake_manager.get_data_frame("my_table")
            ```
        """
        params = TableNameModel(table_name=table_name)
        self._check_table_exists(table_name=params.table_name)
        return self.table_manager.get_data_frame(table_name=params.table_name)

    def get_lazy_frame(self, table_name: str) -> pl.LazyFrame:
        """
        Get a lazy DataFrame from a table for deferred execution.

        Args:
            table_name: Name of the table to read

        Returns:
            A Polars LazyFrame referencing the table data

        Notes:
            - LazyFrames allow for deferred execution and optimization of query plans.
            - Table data is not loaded into memory until an action (like collect) is called.
            - This is suitable for large tables or complex queries.

        Example:
            ```python
            lazy_frame = lake_manager.get_lazy_frame("my_table")
            result = lazy_frame.filter(col("column") > 10).select(["column"])
            result.collect()
            ```
        """
        params = TableNameModel(table_name=table_name)
        self._check_table_exists(table_name=params.table_name)
        return self.table_manager.get_lazy_frame(table_name=params.table_name)

    def optimize_table(
        self,
        table_name: str,
        target_size: int = 512 * 1024 * 1024,
        max_concurrent_tasks: Optional[int] = None,
        writer_properties: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Optimize a table by compacting small files in to files of the target size.
        Optimizing a table can improve query performance and cloud costs.

        Args:
            table_name: Name of the table to optimize
            target_size: Target file size in bytes for optimization
            max_concurrent_tasks: Maximum number of concurrent tasks for optimization
            writer_properties: Optional writer properties for optimization

        Notes:
            - The target size is the desired size of the output files after optimization.
            - The default target size is 512 MB (512 * 1024 * 1024 bytes).
            - The optimization process may take some time depending on the size of the table and the number of files.

        Example:
            ```python
            lake_manager.optimize_table("my_table", target_size=512*1024*1024)
            ```
        """
        params = OptimizeTableModel(
            table_name=table_name,
            target_size=target_size,
            max_concurrent_tasks=max_concurrent_tasks,
            writer_properties=writer_properties,
        )

        self._check_table_exists(table_name=params.table_name)
        self.table_manager.optimize_table(
            table_name=params.table_name,
            target_size=params.target_size,
            max_concurrent_tasks=params.max_concurrent_tasks,
            writer_properties=params.writer_properties,
        )

    def vacuum_table(
        self,
        table_name: str,
        retention_hours: Optional[int] = 168,
        enforce_retention_duration: Optional[bool] = False,
    ) -> None:
        """
        Clean up old data files from a table based on the retention period.
        Old data files are those that are no longer referenced by the table.

        Args:
            table_name: Name of the table to vacuum
            retention_hours: Retention period in hours (0 means delete all unreferenced files)
            enforce_retention_duration: Whether to enforce the retention period

        Notes:
            - The retention period is the time duration for which files are retained.
            - Files older than the retention period will be deleted.
            - Setting retention_hours to 0 will delete all unreferenced files, regardless of age.
            - The enforce_retention_duration flag ensures that the retention period is strictly enforced.
            - Use caution when setting retention_hours to 0, as this will delete all unreferenced files.
            - This operation is irreversible, deleted files cannot be recovered.
            - The vacuum operation may take some time depending on the size of the table and the number of files.

        Example:
            ```python
            lake_manager.vacuum_table("my_table", retention_hours=24)
            ```
        """
        params = VacuumTableModel(
            table_name=table_name,
            retention_hours=retention_hours,
            enforce_retention_duration=enforce_retention_duration,
        )

        self._check_table_exists(table_name=params.table_name)
        self.table_manager.vacuum_table(
            table_name=params.table_name,
            retention_hours=params.retention_hours,
            enforce_retention_duration=params.enforce_retention_duration,
        )

    def list_tables(self) -> Dict[str, Dict[str, Any]]:
        """
        List all tables in the data lake.

        Returns:
            A dictionary mapping table names to their metadata

        Example:
            ```python
            lake_manager.list_tables()
            ```
        """
        return self.table_manager.list_tables()

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific table.

        Args:
            table_name: Name of the table to get information for

        Returns:
            A dictionary containing detailed table information

        Example:
            ```python
            lake_manager.get_table_info("my_table")
            ```
        """
        params = TableNameModel(table_name=table_name)
        self._check_table_exists(table_name=params.table_name)
        return self.table_manager.get_table_info(table_name=params.table_name)

    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get the schema definition for a specific table.

        Args:
            table_name: Name of the table to get the schema for

        Returns:
            A dictionary representing the table schema

        Example:
            ```python
            lake_manager.get_table_schema("my_table")
            ```
        """
        params = TableNameModel(table_name=table_name)
        self._check_table_exists(table_name=params.table_name)
        return self.table_manager.get_table_schema(table_name=params.table_name)

    def delete_table(self, table_name: str) -> bool:
        """
        Delete a table from the data lake.
        Deleted data files are not recoverable, so use with caution.

        Args:
            table_name: Name of the table to delete

        Returns:
            True if the table was successfully deleted

        Notes:
            - This operation is irreversible, and deleted tables cannot be recovered.
            - Use caution when deleting tables, especially in production environments.
            - Ensure that you have backups or copies of important data before deletion.
            - Deleting a table will remove all associated data files and metadata.

        Example:
            ```python
            lake_manager.delete_table("my_table")
            ```
        """
        params = TableNameModel(table_name=table_name)
        self._check_table_exists(table_name=params.table_name)
        return self.table_manager.delete_table(table_name=params.table_name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class LocalLakeManager(LakeManager):
    """
    Implementation of LakeManager for local filesystem storage.

    This class extends the base LakeManager to provide specific functionality
    for managing Delta tables in a local filesystem.
    """

    def __init__(self, base_path: str):
        """
        Initialize a new LocalLakeManager.

        Args:
            base_path: The local filesystem path where the data lake will be stored

        Example:
            ```python
            from pdldb import LocalLakeManager
            lake_manager = LocalLakeManager("data")
            ```
        """
        params = LakeManagerInitModel(base_path=base_path, storage_options=None)
        super().__init__(params.base_path, params.storage_options)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.table_manager = LocalTableManager(self.base_path, self.storage_options)


class S3LakeManager(LakeManager):
    """
    Implementation of LakeManager for Amazon S3 storage.

    This class extends the base LakeManager to provide specific functionality
    for managing Delta tables in Amazon S3.

    Notes:
        Delta Lake normally guarantees ACID transactions when writing data; this is done
        by default when writing to all supported object stores except AWS S3.

        When writing to S3, there are two approaches:

        1. Using a DynamoDB locking provider (recommended for production):
           This ensures safe concurrent writes (ACID) by using a DynamoDB table to manage locks.

        2. Allowing unsafe renames:
           This approach doesn't guarantee data consistency with concurrent writes.
    """

    def __init__(
        self,
        base_path: str,
        aws_region: str,
        aws_access_key: str,
        aws_secret_key: str,
        dynamodb_locking_table: Optional[str] = None,
    ):
        """
        Initialize a new S3LakeManager.

        Args:
            base_path: The S3 bucket path where the data lake will be stored (e.g., "s3://bucket/prefix/")
            aws_region: AWS region name (e.g., "us-east-1")
            aws_access_key: AWS access key ID
            aws_secret_key: AWS secret access key
            dynamodb_locking_table: Optional name of DynamoDB table to use as locking provider.
                                    If not provided, unsafe renames will be enabled.

        Notes:
            - For production use with concurrent writes, it's strongly recommended to provide
              a DynamoDB table for locking to ensure ACID guarantees.
            - The DynamoDB table must have the following schema:
              - Partition key: 'tablePath' (String)
              - Sort key: 'fileName' (String)
            - If no dynamodb_locking_table is provided, the S3LakeManager will use unsafe
              renames which doesn't guarantee data consistency with concurrent writes.
            - You can create the required DynamoDB table with:
              ```console
              aws dynamodb create-table
                  --table-name delta_log
                  --attribute-definitions AttributeName=tablePath,AttributeType=S AttributeName=fileName,AttributeType=S
                  --key-schema AttributeName=tablePath,KeyType=HASH AttributeName=fileName,KeyType=RANGE
                  --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
              ```
            - Consider setting a TTL on the DynamoDB table to avoid it growing indefinitely.

        Example:
            ```python
            from pdldb import S3LakeManager

            # Using unsafe renames (not safe for concurrent writes)
            lake_manager = S3LakeManager(
                "s3://mybucket/mydatalake/",
                aws_region="us-east-1",
                aws_access_key="YOUR_ACCESS_KEY",
                aws_secret_key="YOUR_SECRET_KEY"
            )

            # Using DynamoDB locking provider (safe for concurrent writes)
            lake_manager = S3LakeManager(
                "s3://mybucket/mydatalake/",
                aws_region="us-east-1",
                aws_access_key="YOUR_ACCESS_KEY",
                aws_secret_key="YOUR_SECRET_KEY",
                dynamodb_locking_table="delta_log"
            )
            ```
        """
        storage_options = {
            "AWS_REGION": aws_region,
            "AWS_ACCESS_KEY_ID": aws_access_key,
            "AWS_SECRET_ACCESS_KEY": aws_secret_key,
        }

        if dynamodb_locking_table:
            storage_options["AWS_S3_LOCKING_PROVIDER"] = "dynamodb"
            storage_options["DELTA_DYNAMO_TABLE_NAME"] = dynamodb_locking_table
        else:
            storage_options["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"

        params = LakeManagerInitModel(
            base_path=base_path, storage_options=storage_options
        )
        super().__init__(params.base_path, params.storage_options)
        self.table_manager = S3TableManager(str(self.base_path), self.storage_options)
