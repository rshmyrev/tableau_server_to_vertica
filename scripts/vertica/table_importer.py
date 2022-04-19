import csv
import gc
import io
import json
import logging
import warnings
from pathlib import Path

from converter.database_worker import DataBaseWorker
from vconnector.vertica_connector import VerticaConnector

from .config_files.config_netology import Config

warnings.filterwarnings("ignore")

logging.captureWarnings(True)


def myconverter(o):
    return o.__str__()


def write_list_as_json_str(fields_json, rows):
    if fields_json is not None:
        for r in rows:
            for field_json in fields_json:
                if r[field_json] is not None:
                    r[field_json] = json.loads(r[field_json])
    return "\n".join([json.dumps(dict(r), default=myconverter) or "{}" for r in rows])


def write_list_as_csv_str(fields, rows):
    """save data as string, csv to stdin"""
    output = io.StringIO()
    writer = csv.DictWriter(
        output, fieldnames=fields, quoting=csv.QUOTE_NONNUMERIC, delimiter=","
    )
    for row in rows:
        writer.writerow(row)
    return output.getvalue()


class TableImporter(Config):
    def __init__(self, fields_names):
        """init connections to upload"""
        Config.__init__(self)
        self.vertica_fields_names = fields_names
        self.json_fields = []

    def get_fields(self):
        """get columns of source or destiny table"""
        self.db_worker_to = DataBaseWorker(
            sql_credentials=self.sql_credentials,
            db="vertica",
            tables=[self.table_name]
        )

    def extract_full(self, df_json):
        """main function to upload"""
        self.get_fields()

        end = 1
        with VerticaConnector(
                user=self.vertica_user,
                password=self.vertica_password,
                database=self.vertica_database,
                vertica_configs=self.vertica_configs,
                sec_to_recconect=2,
                count_retries=3,
        ) as v_connector:
            cursor_vertica = v_connector.cnx.cursor("dict")
            v_connector.create_staging_table(
                table_name=self.table_name,
                schema=self.vertica_schema,
                staging_schema=self.vertica_schema_staging,
                ddl_path=str(Path.cwd().parent / 'db' / 'vertica' / self.vertica_schema)
            )

            for step_num in range(0, end):
                logging.info("Uploading to vertica")
                sql_copy = """COPY {schema}.{table_name} ({columns}) FROM STDIN PARSER FJSONPARSER(
                    RECORD_TERMINATOR=E'\n', flatten_maps=false) ENFORCELENGTH  ABORT ON ERROR""".format(
                    schema=self.vertica_schema_staging,
                    table_name=self.table_name,
                    columns=",".join(
                        ['"' + v + '"' for v in self.vertica_fields_names]
                    ),
                )
                logging.info(sql_copy)
                cursor_vertica.copy(sql_copy, df_json)
                logging.info("Uploading to vertica success")
                del df_json
                gc.collect()

            v_connector.reload_main_table(
                table_name=self.table_name,
                schema=self.vertica_schema,
                staging_schema=self.vertica_schema_staging
            )

        logging.info("uploaded")
