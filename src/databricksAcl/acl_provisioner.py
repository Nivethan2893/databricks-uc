from databricksAcl.config_helper import ConfigHelper
from pyspark.sql import SparkSession

class ACLProvisioner:
    """
    Generates ACL GRANT/REVOKE statements based on workspace environment and permissions YAML config.
    """

    def __init__(self, spark: SparkSession, yaml_config_path, column_mask_path=None):
        self.spark = spark
        self.logger, _ = ConfigHelper.setup_logging(self.__class__.__name__)
        self.yaml_config_path = yaml_config_path
        self.config = ConfigHelper.load_yaml_config(self.yaml_config_path, self.logger)
        self.env = ConfigHelper.get_environment(self.config, self.logger)
        self.perm_config = self.config["permission_mapping"]
        self.catalog_map = self.config["catalog_mapping"]

        # Optional column masking config
        self.column_mask_path = column_mask_path
        self.column_mask_config = (ConfigHelper.load_yaml_config(column_mask_path, self.logger) if column_mask_path else {})

    def get_permissions(self, permission_group, object_type):
        """
        Returns the list of permissions based on group, env, and object type.

        Args:
            permission_group (str): e.g., 'DATA_READER'
            object_type (str): 'TABLE', 'VOLUME', etc.

        Returns:
            list[str]: Allowed permissions
        """
        permissions = self.perm_config.get(self.env, {}).get(permission_group, [])

        if permission_group == "DATA_READER":
            if object_type == "TABLE":
                return [p for p in permissions if p == "SELECT"]
            elif object_type == "VOLUME":
                return [p for p in permissions if p == "READ VOLUME"]

        return permissions

    def get_tables_from_tags(self, tag_values):
        """
        Finds schema references that match given tag values.

        Args:
            tag_values (list[str])

        Returns:
            list[str]: Catalog.schema list
        """
        conditions = " OR ".join([f"array_contains(split(tag_value, ','), '{tag.strip()}')" for tag in tag_values])

        query = f"""
            SELECT DISTINCT catalog_name, schema_name 
            FROM system.information_schema.schema_tags 
            WHERE {conditions}
        """

        table_list = []
        try:
            df = self.spark.sql(query)
            for row in df.collect():
                table_list.append(f"{row['catalog_name']}.{row['schema_name']}")
        except Exception as e:
            self.logger.error(f"Error fetching tables by tags: {e}")

        return table_list

    def get_tables_from_schema_names(self, schema_names):
        """
        Resolves schema references like 'raw.customer' into catalog-qualified form.

        Args:
            schema_names (list[str])

        Returns:
            list[str]
        """
        table_list = []
        env_map = self.catalog_map.get(self.env, {})

        for ref in schema_names:
            try:
                parts = ref.split(".")
                if len(parts) != 2:
                    self.logger.warning(f"Invalid schema format: {ref}")
                    continue

                prefix, suffix = parts
                catalog = env_map.get(prefix)
                if not catalog:
                    self.logger.warning(f"No catalog mapping found for: {prefix}")
                    continue

                table_list.append(f"{catalog}.{suffix}")
            except Exception as e:
                self.logger.error(f"Error resolving schema name {ref}: {e}")

        return table_list

    def get_tables_from_names(self, table_names):
        """
        Resolves table references like 'raw.customer.data' into catalog.schema.table.

        Args:
            table_names (list[str])

        Returns:
            list[str]
        """
        table_list = []
        env_map = self.catalog_map.get(self.env, {})

        for ref in table_names:
            try:
                parts = ref.split(".")
                if len(parts) != 3:
                    self.logger.warning(f"Invalid table format: {ref}")
                    continue

                prefix, schema, table = parts
                catalog = env_map.get(prefix)
                if not catalog:
                    self.logger.warning(f"No catalog mapping found for: {prefix}")
                    continue

                table_list.append(f"{catalog}.{schema}.{table}")
            except Exception as e:
                self.logger.error(f"Error resolving table name {ref}: {e}")

        return table_list

    def generate_acl_statements(self, table_name, bu):
        """
        Parses access control entries from a Delta table and returns SQL ACL statements.

        Automatically:
        - Updates expired access to 'REVOKE' based on 'access_until'
        - Updates 'updated_datetime' during each ACL processing cycle

        Args:
            table_name (str): Fully qualified Delta table name (e.g., catalog.schema.table)

        Returns:
            DataFrame: Spark DataFrame with a single column 'access_statement'
        """
        # Step 1: Update expired access control entries
        updated_df = ConfigHelper.update_expired_access_and_timestamp(table_name, bu, self.logger)

        # Step 2: Generate ACL SQL statements from updated rows
        access_statements = []

        for row in updated_df.collect():
            grant_type = row["grant_type"].strip()
            object_type = row["object_type"].strip()
            permission_group = row["permission_types"].strip()
            user_groups = [ug.strip() for ug in row["user_group"].split(",") if ug.strip()]

            tag_values_raw = row["tag_values"]
            tag_values = [tag.strip() for tag in tag_values_raw.split(",")] if tag_values_raw else []

            # Fetch permission list for this group + object type + env
            permission_types = self.get_permissions(permission_group, object_type)
            if not permission_types:
                self.logger.warning(
                    f"No permissions mapped for group '{permission_group}' in env '{self.env}' and object_type '{object_type}'"
                )
                continue

            table_list = []

            if tag_values:
                table_list += self.get_tables_from_tags(tag_values)

            if row["schema_names"]:
                schema_names = [s.strip() for s in row["schema_names"].split(",")]
                table_list += self.get_tables_from_schema_names(schema_names)

            if row["table_names"]:
                table_names = [t.strip() for t in row["table_names"].split(",")]
                table_list += self.get_tables_from_names(table_names)

            # Attach tag_values for downstream processing (e.g., update_datetime)
            joined_tag_values = ",".join(tag_values) if tag_values else None

            for table in table_list:
                for group in user_groups:
                    for perm in permission_types:
                        stmt = f"{grant_type} {perm} ON {object_type} {table} {'TO' if grant_type == 'GRANT' else 'FROM'} `{group}`"
                        access_statements.append((stmt, joined_tag_values))

        self.logger.info(f"✅ Generated {len(access_statements)} access statements.")
        return self.spark.createDataFrame(access_statements, ["access_statement", "tag_values"])
    
    
    def apply_column_masking_from_yaml(self):
        
        """
        Applies or drops column-level masks based on YAML configuration under 'column_masks'.
        Supports dynamic schema resolution and multiple user groups.
        """
        column_masks = self.column_mask_config.get("column_masks", [])
        if not column_masks:
            self.logger.info("No column masking rules to apply.")
            return
        env_schema_map = self.catalog_map.get(self.env, {})

        for entry in column_masks:
            try:
                table_ref = entry["table_name"]  
                parts = table_ref.split(".")
                if len(parts) != 3:
                    self.logger.warning(f"Invalid table reference: {table_ref}")
                    continue

                prefix, schema, table = parts
                catalog = env_schema_map.get(prefix)
                if not catalog:
                    self.logger.warning(f"No catalog mapping found for: {prefix}")
                    continue

                full_table_name = f"{catalog}.{schema}.{table}"
                column = entry["column_name"]
                groups = entry.get("user_group")
                mask_value = entry.get("type_of_masking", "****")
                drop_existing = entry.get("drop_existing", False)

                if not groups:
                    self.logger.warning(f"No user group defined for mask on {full_table_name}.{column}")
                    continue

                group_list = [g.strip() for g in groups.split(",")] if isinstance(groups, str) else groups
                group_condition = " OR ".join([f"is_member('{g}')" for g in group_list])

                func_name = f"{table}_{column}_mask".replace("-", "_")

                if drop_existing:
                    drop_mask_sql = f"ALTER TABLE {full_table_name} ALTER COLUMN {column} DROP MASK"
                    self.spark.sql(drop_mask_sql)
                    self.logger.info(f"🧹 Dropped masking from {full_table_name}.{column}")
                    continue

                # Check if function already exists
                self.spark.sql(f"USE CATALOG {catalog}")
                func_check_sql = f"SHOW FUNCTIONS IN {schema}"
                existing_funcs = [row['function'] for row in self.spark.sql(func_check_sql).collect() if row['function'] == f'{catalog}.{schema}.{func_name}'.lower()]
                if func_name.lower() in existing_funcs[0].split(".")[2]:
                    self.logger.info(f"⏭️ Skipped: Function '{func_name}' already exists.")
                    continue
                else:
                    create_func_sql = f"""
                        CREATE FUNCTION {catalog}.{schema}.{func_name}({column} STRING)
                        RETURN CASE
                            WHEN {group_condition} THEN {column}
                            ELSE '{mask_value}'
                        END
                    """
                    self.spark.sql(create_func_sql)
                    self.logger.info(f"✅ Created masking function: {func_name}")

                    apply_mask_sql = f"""
                        ALTER TABLE {full_table_name}
                        ALTER COLUMN {column}
                        SET MASK {catalog}.{schema}.{func_name}
                    """
                    self.spark.sql(apply_mask_sql)
                    self.logger.info(f"✅ Applied masking to {full_table_name}.{column} using: {func_name}")

            except Exception as e:
                self.logger.error(f"❌ Error processing column mask entry {entry}: {e}")