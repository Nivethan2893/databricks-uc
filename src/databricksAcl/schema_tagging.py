from databricksAcl.logging import setup_logging
import yaml

class SchemaTagManager:
    """
    Manages schema and table tags dynamically based on Databricks workspace environment.
    """

    def __init__(self, yaml_path):
        self.logger, _ = setup_logging(self.__class__.__name__)
        self.yaml_path = yaml_path
        self.config = self.load_config()
        self.env = self.get_environment()

    def load_config(self):
        """Loads YAML configuration from the specified path."""
        try:
            with open(self.yaml_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            raise RuntimeError(f"Failed to load YAML file: {e}")

    def get_environment(self):
        """Determines the environment name based on workspace ID from YAML config."""
        try:
            ws_id = spark.conf.get("spark.databricks.workspaceUrl").split("adb-")[1].split(".")[0]
            env_map = self.config.get("env_map", {})
            env = env_map.get(ws_id)
            if not env:
                raise ValueError(f"Workspace ID {ws_id} not found in env_map.")
            self.logger.info(f"Resolved environment '{env}' for workspace ID {ws_id}")
            return env
        except Exception as e:
            raise RuntimeError(f"Failed to determine environment: {e}")

    def _generate_set_tags_sql(self, tags: dict, target_type: str, target: str):
        tag_properties = ", ".join([f"'{k}' = '{v}'" for k, v in tags.items()])
        return f"ALTER {target_type} {target} SET TAGS ({tag_properties})"

    def _generate_unset_tags_sql(self, tags: dict, target_type: str, target: str):
        tag_keys = ", ".join([f"'{tag}'" for tag in tags.keys()])
        return f"ALTER {target_type} {target} UNSET TAGS ({tag_keys})"

    def apply_schema_tags(self):
        """Applies or removes tags at the schema level."""
        schema_tags = self.config.get("schema_tags", [])
        for entry in schema_tags:
            action = entry["action"].upper()
            schemas = entry["schemas"]
            tags = entry["tags"]

            schemas = [schemas] if isinstance(schemas, str) else schemas
            schemas = [schema.replace("xxx", self.env) for schema in schemas]

            for schema in schemas:
                try:
                    if not isinstance(tags, dict):
                        raise ValueError(f"For '{action}', tags must be a dict.")
                    
                    if action == "SET":
                        sql_cmd = self._generate_set_tags_sql(tags, "SCHEMA", schema)
                    elif action == "UNSET":
                        sql_cmd = self._generate_unset_tags_sql(tags, "SCHEMA", schema)
                    else:
                        self.logger.warning(f"Unsupported action '{action}' for schema tagging.")
                        continue

                    spark.sql(sql_cmd)
                    self.logger.info(f"{action} tags {tags} on schema {schema}")

                except Exception as e:
                    self.logger.error(f"Error processing schema {schema}: {e}")

    def apply_table_tags(self):
        """Applies or removes tags at the table level, if specified in the YAML config."""
        table_tags = self.config.get("table_tags", [])
        for entry in table_tags:
            action = entry["action"].upper()
            schemas = entry["schemas"]
            tables = entry["tables"]
            tags = entry["tags"]

            schemas = [schemas] if isinstance(schemas, str) else schemas
            schemas = [schema.replace("xxx", self.env) for schema in schemas]
            tables = [tables] if isinstance(tables, str) else tables

            for schema in schemas:
                for table in tables:
                    full_table_name = f"{schema}.{table}"
                    try:
                        if not isinstance(tags, dict):
                            raise ValueError(f"For '{action}', tags must be a dict.")
                        
                        if action == "SET":
                            sql_cmd = self._generate_set_tags_sql(tags, "TABLE", full_table_name)
                        elif action == "UNSET":
                            sql_cmd = self._generate_unset_tags_sql(tags, "TABLE", full_table_name)
                        else:
                            self.logger.warning(f"Unsupported action '{action}' for table tagging.")
                            continue

                        spark.sql(sql_cmd)
                        self.logger.info(f"{action} tags {tags} on table {full_table_name}")

                    except Exception as e:
                        self.logger.error(f"Error processing table {full_table_name}: {e}")

    def apply_all_tags(self):
        """Applies both schema-level and table-level tags as specified in YAML config."""
        self.logger.info("Starting schema-level tag application...")
        self.apply_schema_tags()

        self.logger.info("Starting table-level tag application...")
        self.apply_table_tags()

        self.logger.info("Completed applying all tags.")