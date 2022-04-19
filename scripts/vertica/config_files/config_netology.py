import json
import os


class Config:
    def __init__(self):
        self.vertica_host = os.getenv("VERTICA_HOST")
        self.vertica_port = os.getenv("VERTICA_PORT")
        self.vertica_user = os.getenv("VERTICA_USER_W")
        self.vertica_password = os.getenv("VERTICA_PASSWORD_W")
        self.vertica_database = "DWH"
        self.vertica_schema_staging = "netology_staging"
        self.vertica_schema = "netology_temp"
        self.vertica_configs = json.loads(os.getenv("VERTICA_CONFIGS"))

        # config for table converter
        self.sql_credentials = {
            "vertica": {
                "database"    : self.vertica_database,
                "schema"      : self.vertica_schema,
                "user"        : self.vertica_user,
                "host"        : self.vertica_host,
                "port"        : self.vertica_port,
                "password"    : self.vertica_password,
                "connect_args": {
                    "connection_load_balance": True,
                    "backup_server_node"     : self.vertica_configs["backup_server_node"],
                },
            },
        }
        self.table_name = os.getenv("table_name")
        if self.table_name is None:
            raise Exception(
                "Error. You must to add at least a name of the table_name  in dags/config/config.json"
            )
        try:
            self.step = int(os.getenv("step"))
        except TypeError:
            self.step = 250000
        self.json_columns = os.getenv("json_column")
        if self.json_columns is not None:
            self.json_columns = self.json_columns.split(',')
        self.key_column = os.getenv("key_column") or "id"
