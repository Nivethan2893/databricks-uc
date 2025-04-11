import os
import yaml
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.functions import *
import re

class ConfigHelper:
    @staticmethod
    def load_yaml_config(yaml_path, logger=None):
        try:
            with open(yaml_path, "r") as f:
                return yaml.safe_load(f)
        except Exception as e:
            if logger:
                logger.error(f"Failed to load YAML config: {e}")
            raise

    @staticmethod
    def get_environment(config, logger=None):
        try:
            ws_id = spark.conf.get("spark.databricks.workspaceUrl").split("adb-")[1].split(".")[0]
            env_map = config.get("env_map", {})
            env = env_map.get(ws_id)
            if not env:
                raise ValueError(f"Workspace ID {ws_id} not found in env_map.")
            if logger:
                logger.info(f"Resolved environment '{env}' from workspace ID {ws_id}")
            return env
        except Exception as e:
            if logger:
                logger.error(f"Failed to determine environment: {e}")
            raise

    @staticmethod
    def setup_logging(name, log_dir="logs"):
        """
        Sets up logging for Databricks jobs and notebooks.

        Parameters:
        - name (str): Logger name (e.g., class or function name)
        - log_dir (str or None): If provided, logs are written to this DBFS/UC directory.

        Returns:
        - Tuple[logging.Logger, Optional[logging.Handler]]
        """

        # Create a logger with the name of the module
        logger = logging.getLogger(name)

        # Always set level regardless of reuse
        logger.setLevel(logging.INFO)

        # Prevent duplicate handlers
        if not logger.handlers:
            os.makedirs(log_dir, exist_ok=True)

            log_format = "%(asctime)s %(levelname)s [%(name)s] {%(threadName)s} %(message)s"
            formatter = logging.Formatter(fmt=log_format, datefmt="%H:%M:%S")

            # Console logging (for Databricks notebook)
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            stream_handler.setLevel(logging.INFO)
            logger.addHandler(stream_handler)

            # File logging
            log_file = os.path.join(log_dir, f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
            file_handler = TimedRotatingFileHandler(log_file, when="m", interval=5, backupCount=5)
            file_handler.setFormatter(formatter)
            file_handler.setLevel(logging.INFO)
            logger.addHandler(file_handler)

            logger.propagate = False  # Prevent duplicate messages in root logger

            return logger, file_handler

        return logger, None

    @staticmethod
    def update_expired_access_and_timestamp(table_name: str, bu: str, logger=None):
        """
        Updates the Delta table rows where `access_until` has expired by:
        - Changing `grant_type` to 'REVOKE' if expired
        - Updating `updated_datetime` to current timestamp

        This uses Delta Lake's MERGE to only update matching rows, preserving all others.

        Args:
            table_name (str): Fully qualified Delta table name
            logger (logging.Logger, optional): Logger instance for audit/debug

        Returns:
            DataFrame: Updated DataFrame after applying expiry/revoke logic
        """
        try:
            df = spark.table(table_name).filter(col("bu") == bu).filter(col("grant_type") == "GRANT")

            # Prepare updated version with revoke logic and new timestamp
            updated_df = df.withColumn("grant_type", when(
                        col("access_until").isNotNull() & (col("access_until") < current_date()),
                        lit("REVOKE")
                    ).otherwise(col("grant_type"))
                ).withColumn("updated_datetime", current_timestamp())

            # Merge only if table exists and is Delta
            delta_table = DeltaTable.forName(spark, table_name)

            # Define null-safe merge condition (excluding timestamps)
            merge_condition = """
                source.object_type = target.object_type AND
                source.user_group = target.user_group AND
                source.permission_types = target.permission_types AND
                source.schema_names <=> target.schema_names AND
                source.table_names <=> target.table_names AND
                source.tag_values <=> target.tag_values AND
                source.grant_type != target.grant_type
            """

            delta_table.alias("target").merge(
                updated_df.alias("source"),
                merge_condition
            ).whenMatchedUpdate(set={
                "grant_type": "source.grant_type",
                "updated_datetime": "source.updated_datetime"
            }).execute()

            logger.info(f"Updated expired grants (set to REVOKE) in table: {table_name}")

            return updated_df

        except Exception as e:
            logger.error(f"Failed to update expired access rows in table '{table_name}': {e}")
            raise
    
    @staticmethod
    def append_unique_acl_rows_from_mapping_file(acl_mapping_path, bu, target_table, target_table_path, logger=None):
        """
        Loads ACL records from a CSV and appends only new rows to the target Delta table,
        based on all column values (exact match).

        Args:
            acl_mapping_path (str): Path to the input mapping file
            bu (str): Business Unit
            target_table (str): Fully qualified Delta table name (e.g. catalog.schema.table)
            target_table_path (str): Path to the target Delta table
        """

        # Step 1: Read the incoming CSV
        sheet_name = f"{bu}_AAD_ACL_Mapping"

        # Read specific sheet using com.crealytics.spark.excel
        new_df = spark.read.format("com.crealytics.spark.excel") \
            .option("dataAddress", f"'{sheet_name}'!A1") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(acl_mapping_path) \
            .withColumn("access_until", date_format(col("access_until"), "yyyy-MM-dd"))

        # Add processed timestamp, updated timestamp and bu columns
        cleaned_df = new_df.withColumn("processed_datetime", current_timestamp()) \
                    .withColumn("updated_datetime", lit("").cast("timestamp")) \
                    .withColumn("bu", lit(bu).cast("string"))
        
        try:
            # Step 2: If the table exists, merge; else create
            if DeltaTable.isDeltaTable(spark, target_table_path):
                delta_table = DeltaTable.forPath(spark, target_table_path)


                merge_condition = """
                    source.object_type = target.object_type AND
                    source.user_group = target.user_group AND
                    source.tag_values <=> target.tag_values AND
                    source.schema_names <=> target.schema_names AND
                    source.table_names <=> target.table_names AND
                    source.permission_types = target.permission_types AND
                    source.grant_type = target.grant_type AND
                    source.access_until <=> target.access_until AND
                    source.bu = target.bu
                """

                delta_table.alias("target") \
                    .merge(cleaned_df.alias("source"), merge_condition) \
                    .whenMatchedUpdate(set={
                        "object_type": "source.object_type",
                        "user_group": "source.user_group",
                        "tag_values": "source.tag_values",
                        "schema_names": "source.schema_names",
                        "table_names": "source.table_names",
                        "permission_types": "source.permission_types",
                        "grant_type": "source.grant_type",
                        "access_until": "source.access_until"
                    }) \
                    .whenNotMatchedInsertAll() \
                    .execute()

                logger.info(f"Table '{target_table}' updated with inserts and updates as needed.")
            else:
                cleaned_df.write.format("delta").mode("overwrite").option("path", target_table_path).saveAsTable(target_table)
                logger.info(f"Table '{target_table}' created and initial data inserted.")

        except Exception as e:
            logger.error(f"Failed to update access control table '{target_table}': {e}")