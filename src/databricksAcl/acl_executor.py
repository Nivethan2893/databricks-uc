from databricksAcl.config_helper import ConfigHelper
import re
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, lit, concat_ws, col, split, collect_set, sort_array
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

class ACLExecutor:
    """
    Executes ACL GRANT/REVOKE statements and updates the ACL Delta tracking table accordingly.
    """

    def __init__(self, spark: SparkSession, table_name):
        """
        Initializes the ACLExecutor with config, logging, and target ACL Delta table.

        Args:
            table_name (str): Delta table tracking executed ACLs
        """
        self.spark = spark
        self.logger, _ = ConfigHelper.setup_logging(self.__class__.__name__)
        self.table_name = table_name

    def parse_acl_statement(self, sql: str, tag_values: str = None) -> dict:
        """
        Parses a GRANT/REVOKE SQL statement into structured fields.

        Args:
            sql (str): SQL string like "GRANT SELECT ON TABLE catalog.schema.table TO `group`"
            tag_values (str or None): Optional comma-separated tag values used for schema access resolution

        Returns:
            dict or None: Parsed components as dictionary, or None if pattern doesn't match
        """
        try:
            pattern = r"(GRANT|REVOKE)\s+(.*?)\s+ON\s+(\w+)\s+([\w.]+)\s+(TO|FROM)\s+`([^`]+)`"
            match = re.match(pattern, sql.strip())

            if match:
                return {
                    "grant_type": match.group(1),
                    "permissions": match.group(2),
                    "object_type": match.group(3),
                    "object_name": match.group(4),
                    "user_group": match.group(6),
                    "tag_values": tag_values
                }

            else:
                self.logger.warning(f"⚠️ Unable to parse ACL SQL: {sql}")
                return None

        except Exception as e:
            self.logger.error(f"❌ Error parsing ACL statement: {sql} | Error: {e}")
            return None

    def batch_update_updated_datetime(self, parsed_rows):
        """
        Batch updates `updated_datetime` for all successfully executed ACL rows.
        Supports tag_values-based matching as well as schema/table resolution.
        """
        try:
            # Define schema explicitly to avoid type inference issues
            schema = StructType([
                StructField("grant_type", StringType(), True),
                StructField("permissions", StringType(), True),
                StructField("object_type", StringType(), True),
                StructField("object_name", StringType(), True),
                StructField("user_group", StringType(), True),
                StructField("tag_values", StringType(), True)
            ])

            df = self.spark.createDataFrame(parsed_rows, schema) \
                  .withColumn("updated_datetime", current_timestamp())

            if df.count() == 0:
                self.logger.warning("⚠️ No parsed ACL entries to update.")
                return

            delta_table = DeltaTable.forName(self.spark, self.table_name)

            for object_type in ["TABLE", "SCHEMA", "VOLUME"]:
                subset_df = df.filter(col("object_type") == object_type)

                if subset_df.isEmpty():
                    continue

                if object_type == "SCHEMA":
                    # tag-based entries
                    subset_df_schema1 = subset_df.filter("tag_values is not null") \
                        .select("object_type", "user_group", "tag_values", "updated_datetime") \
                        .withColumn("schema_names", lit(None).cast("string")) \
                        .withColumn("table_names", lit(None).cast("string")) \
                        .distinct()

                    # resolved schema_names (e.g., raw.schema) ➝ catalog.schema
                    subset_df_schema2 = subset_df.filter("tag_values is null") \
                        .select("object_type", "user_group", col("object_name").alias("schema_names"), "updated_datetime") \
                        .withColumn("schema_names", concat_ws(".", split(split(col("schema_names"), "\\.")[0], "\\_")[2], split(col("schema_names"), "\\.")[1])) \
                        .withColumn("tag_values", lit(None).cast("string")) \
                        .withColumn("table_names", lit(None).cast("string")) \
                        .groupBy("object_type", "user_group", "tag_values", "updated_datetime", "table_names") \
                        .agg(concat_ws(",", collect_set("schema_names")).alias("schema_names")).distinct()

                    subset_df_schema = subset_df_schema1.unionByName(subset_df_schema2)

                    merge_condition = """
                        source.object_type = target.object_type AND
                        source.user_group = target.user_group AND
                        target.tag_values <=> source.tag_values AND
                        target.schema_names <=> source.schema_names
                    """

                    delta_table.alias("target").merge(
                    subset_df_schema.alias("source"),
                    merge_condition).whenMatchedUpdate(set={
                    "updated_datetime": "source.updated_datetime"}).execute()

                if object_type == "TABLE":
                    # catalog.schema.table ➝ resolve to full table name
                    subset_df_table = subset_df \
                        .select("object_type", "user_group", col("object_name").alias("table_names"), "updated_datetime") \
                        .withColumn("table_names", concat_ws(".", split(split(col("table_names"), "\\.")[0], "\\_")[2], split(col("table_names"), "\\.")[1], split(col("table_names"), "\\.")[2])) \
                        .withColumn("tag_values", lit(None).cast("string")) \
                        .withColumn("schema_names", lit(None).cast("string")) \
                        .groupBy("object_type", "user_group", "tag_values", "updated_datetime", "schema_names") \
                        .agg(concat_ws(",", sort_array(collect_set("table_names"))).alias("table_names")).distinct()

                    merge_condition = """
                        source.object_type = target.object_type AND
                        source.user_group = target.user_group AND
                        target.table_names <=> source.table_names
                    """

                    delta_table.alias("target").merge(
                    subset_df_table.alias("source"),
                    merge_condition).whenMatchedUpdate(set={
                    "updated_datetime": "source.updated_datetime"}).execute()

                if object_type == "VOLUME":
                    # Add similar logic for VOLUME if needed
                    self.logger.info("Volume logic not yet implemented.")
                    continue

            self.logger.info(f"✅ Batch updated {len(parsed_rows)} rows with updated_datetime.")

        except Exception as e:
            self.logger.error(f"Batch update of updated_datetime failed: {e}")

    def execute_sql_statements(self, granting_bu, grant_df, dry_run=False, log_file=None):
        """
        Executes SQL statements from a given Spark DataFrame.

        Parameters:
        - granting_bu (str): Business unit name for logger naming
        - grant_df (DataFrame): Spark DataFrame containing SQL statements to execute
        - dry_run (bool): If True, only logs the statements but doesn't execute them
        - log_file (str or None): Optional log directory (e.g., '/dbfs/tmp/acl_logs')

        Returns:
        - dict: Summary of execution (total, success, failure)
        """
        try:
            logger_name = f"Granting_access_to_{granting_bu}"
            logger, file_handler = ConfigHelper.setup_logging(logger_name, log_file)

            total = grant_df.count()
            success_count = 0
            failure_count = 0
            successful_rows = []

            logger.info(f"{'DRY RUN - ' if dry_run else ''}Starting execution of {total} SQL statements...")

            for row in grant_df.collect():
                sql_command = row["access_statement"]
                tag_values = row["tag_values"] if "tag_values" in row else None

                parsed = self.parse_acl_statement(sql_command, tag_values)

                try:
                    if not dry_run:
                        self.spark.sql(sql_command)
                        if parsed:
                            successful_rows.append(parsed)

                    logger.info(f"✅ {'Would execute' if dry_run else 'Executed'}: {sql_command}")
                    success_count += 1
                except Exception as e:
                    logger.error(f"❌ Error executing: {sql_command} | Error: {e}")
                    failure_count += 1

            # Perform batch update after execution
            if not dry_run and successful_rows:
                self.batch_update_updated_datetime(successful_rows)

            logger.info("===== Execution Summary =====")
            logger.info(f"Total statements: {total}")
            logger.info(f"✅ Successfully processed: {success_count}")
            logger.info(f"❌ Failed executions    : {failure_count}")
            logger.info("===== Execution Completed =====")
            
            return {"total": total, "success": success_count, "failure": failure_count}

        except Exception as e:
            logger.error(f"Error: {str(e)}")
        finally:
            # Close the file handler and remove it from the logger
            if file_handler:
                file_handler.close()
                logger.removeHandler(file_handler)