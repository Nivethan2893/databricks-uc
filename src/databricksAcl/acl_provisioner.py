from databricksAcl.logging import setup_logging
import yaml

class ACLProvisioner:
    """
    Generates ACL GRANT/REVOKE statements based on workspace environment and permissions YAML config.
    """

    def __init__(self, yaml_config_path):
        self.logger, _ = setup_logging(self.__class__.__name__)
        self.yaml_config_path = yaml_config_path
        self.config = self.load_yaml_config()
        self.env = self.get_environment()
        self.perm_config = self.config["permission_mapping"]
        self.schema_map = self.config["schema_mapping"]

    def load_yaml_config(self):
        """Loads and parses the YAML config file."""
        try:
            with open(self.yaml_config_path, 'r') as yaml_file:
                return yaml.safe_load(yaml_file)
        except Exception as e:
            self.logger.error(f"Failed to load YAML config: {e}")
            raise

    def get_environment(self):
        """Resolves current environment from workspace ID."""
        try:
            ws_id = spark.conf.get("spark.databricks.workspaceUrl").split("adb-")[1].split(".")[0]
            env_map = {
                "2820692244148839": "dev",
                "2628671957839451": "acc",
                "573860658642293": "prd"
            }
            env = env_map.get(ws_id)
            if not env:
                raise ValueError(f"Workspace ID {ws_id} not found in env_map.")
            self.logger.info(f"Resolved environment '{env}' from workspace ID {ws_id}")
            return env
        except Exception as e:
            self.logger.error(f"Failed to determine environment: {e}")
            raise

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
            df = spark.sql(query)
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
        env_map = self.schema_map.get(self.env, {})

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
        env_map = self.schema_map.get(self.env, {})

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

    def generate_statements(self, table_list, permission_types, object_type, grant_type, user_groups):
        """
        Creates GRANT or REVOKE SQL statements.

        Args:
            table_list (list[str])
            permission_types (list[str])
            object_type (str)
            grant_type (str): 'GRANT' or 'REVOKE'
            user_groups (list[str])

        Returns:
            list[tuple[str]]
        """
        target_keyword = "TO" if grant_type == "GRANT" else "FROM"
        statements = []

        for table in set(table_list):
            for group in user_groups:
                for perm in permission_types:
                    stmt = f"{grant_type} {perm} ON {object_type} {table} {target_keyword} `{group}`"
                    statements.append((stmt,))
        return statements

    def generate_acl_statements(self, csv_path):
        """
        Parses access CSV and returns all SQL ACL statements.

        Args:
            csv_path (str): CSV with user_group, schema/table info, and permissions

        Returns:
            DataFrame: Single-column Spark DataFrame of SQL statements
        """
        df = spark.read.option("header", True).option("delimiter", ";").csv(csv_path)
        access_statements = []

        for row in df.collect():
            grant_type = row["grant_type"].strip()
            object_type = row["object_type"].strip()
            permission_group = row["permission_types"].strip()
            user_groups = [ug.strip() for ug in row["user_group"].split(",") if ug.strip()]

            permission_types = self.get_permissions(permission_group, object_type)
            if not permission_types:
                self.logger.warning(
                    f"No permissions mapped for group '{permission_group}' in env '{self.env}' and object_type '{object_type}'"
                )
                continue

            table_list = []

            if row["tag_values"]:
                tag_values = [tag.strip() for tag in row["tag_values"].split(",")]
                table_list += self.get_tables_from_tags(tag_values)

            if row["schema_names"]:
                schema_names = [s.strip() for s in row["schema_names"].split(",")]
                table_list += self.get_tables_from_schema_names(schema_names)

            if row["table_names"]:
                table_names = [t.strip() for t in row["table_names"].split(",")]
                table_list += self.get_tables_from_names(table_names)

            access_statements += self.generate_statements(
                table_list, permission_types, object_type, grant_type, user_groups
            )

        self.logger.info(f"Generated {len(access_statements)} access statements.")
        return spark.createDataFrame(access_statements, ["access_statement"])
    
    
    def apply_column_masking_from_yaml(self):
        
        """
        Applies or drops column-level masks based on YAML configuration under 'column_masks'.
        Supports dynamic schema resolution and multiple user groups.
        """
        column_masks = self.config.get("column_masks", [])
        env_schema_map = self.schema_map.get(self.env, {})

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
                    spark.sql(drop_mask_sql)
                    self.logger.info(f"🧹 Dropped masking from {full_table_name}.{column}")
                    continue

                # Check if function already exists
                spark.sql(f"USE CATALOG {catalog}")
                func_check_sql = f"SHOW FUNCTIONS IN {schema}"
                existing_funcs = [row['function'] for row in spark.sql(func_check_sql).collect() if row['function'] == f'{catalog}.{schema}.{func_name}'.lower()]
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
                    spark.sql(create_func_sql)
                    self.logger.info(f"✅ Created masking function: {func_name}")

                    apply_mask_sql = f"""
                        ALTER TABLE {full_table_name}
                        ALTER COLUMN {column}
                        SET MASK {catalog}.{schema}.{func_name}
                    """
                    spark.sql(apply_mask_sql)
                    self.logger.info(f"✅ Applied masking to {full_table_name}.{column} using: {func_name}")

            except Exception as e:
                self.logger.error(f"❌ Error processing column mask entry {entry}: {e}")