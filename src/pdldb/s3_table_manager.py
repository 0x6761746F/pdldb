from pdldb.base_table_manager import BaseTableManager
from pdldb.base_table_validator import BaseTable
from deltalake import DeltaTable
from typing import Dict, Optional
import boto3
import os
from urllib.parse import urlparse


class S3TableManager(BaseTableManager):
    def __init__(
        self, delta_table_path: str, storage_options: Optional[Dict[str, str]] = None
    ):
        super().__init__(delta_table_path, storage_options)

        parsed_url = urlparse(delta_table_path)
        if parsed_url.scheme != "s3":
            raise ValueError("S3TableManager requires an S3 URL (s3://...)")

        self.bucket_name = parsed_url.netloc
        
        self.prefix = parsed_url.path.lstrip("/")
        if self.prefix and not self.prefix.endswith("/"):
            self.prefix += "/"
            
        print(f"S3TableManager initialized with bucket: {self.bucket_name}, prefix: {self.prefix}")

        self.s3_client = boto3.client(
            "s3",
            region_name=storage_options.get("AWS_REGION"),
            aws_access_key_id=storage_options.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=storage_options.get("AWS_SECRET_ACCESS_KEY"),
        )

        self._load_existing_tables()

    def _load_existing_tables(self) -> None:
        try:
            print(f"Looking for tables in bucket: {self.bucket_name}, prefix: {self.prefix}")
            
            result = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=self.prefix, Delimiter="/"
            )
            
            if "CommonPrefixes" not in result:
                print(f"No CommonPrefixes found in response. Response keys: {list(result.keys())}")
                if "Contents" in result:
                    print(f"Found {len(result['Contents'])} objects in the prefix")
                return

            print(f"Found {len(result['CommonPrefixes'])} potential table directories")
            
            for prefix in result["CommonPrefixes"]:
                table_prefix = prefix["Prefix"]
                table_name = os.path.basename(table_prefix.rstrip("/"))
                
                print(f"Checking prefix: {table_prefix} for table name: {table_name}")

                delta_log_check = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=f"{table_prefix}_delta_log/",
                    MaxKeys=1,
                )

                if "Contents" in delta_log_check and len(delta_log_check["Contents"]) > 0:
                    print(f"Found Delta table: {table_name}")
                    
                    try:
                        full_table_path = f"s3://{self.bucket_name}/{table_prefix}"
                        print(f"Loading Delta table from path: {full_table_path}")
                        
                        storage_opts = self.storage_options.copy() if self.storage_options else {}
                        print(f"Using storage options: {storage_opts}")
                        
                        dt = DeltaTable(
                            full_table_path,
                            storage_options=storage_opts,
                        )

                        primary_keys = dt.metadata().description or "unknown_primary_keys"
                        print(f"Table {table_name} primary keys: {primary_keys}")

                        pa_schema = dt.schema().to_pyarrow()
                        schema_dict = {field.name: str(field.type) for field in pa_schema}
                        print(f"Table {table_name} schema has {len(schema_dict)} fields")

                        base_table = BaseTable(
                            name=table_name,
                            table_schema=schema_dict,
                            primary_keys=primary_keys,
                        )
                        self.tables[table_name] = base_table
                        print(f"Successfully added table {table_name} to manager")
                    except Exception as e:
                        print(f"Error loading Delta table {table_name}: {str(e)}")
                        import traceback
                        print(traceback.format_exc())
                else:
                    print(f"No _delta_log found for {table_prefix}")

            print(f"Loaded {len(self.tables)} tables: {list(self.tables.keys())}")

        except Exception as e:
            print(f"Error loading existing tables: {str(e)}")
            import traceback
            print(traceback.format_exc())


    def delete_table(self, table_name: str) -> bool:
        try:
            table_path = f"{self.prefix}{table_name}" if self.prefix else table_name

            delta_table = DeltaTable(
                f"s3://{self.bucket_name}/{table_path}",
                storage_options=self.storage_options,
            )
            delta_table.delete()

            objects_to_delete = []
            paginator = self.s3_client.get_paginator("list_objects_v2")

            for page in paginator.paginate(
                Bucket=self.bucket_name, Prefix=f"{table_path}/"
            ):
                if "Contents" in page:
                    objects_to_delete.extend(
                        [{"Key": obj["Key"]} for obj in page["Contents"]]
                    )

            if objects_to_delete:
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i : i + 1000]
                    if batch:
                        self.s3_client.delete_objects(
                            Bucket=self.bucket_name, Delete={"Objects": batch}
                        )

            if table_name in self.tables:
                del self.tables[table_name]

            return True

        except Exception as e:
            print(f"Failed to delete table {table_name}: {e}")
            return False
