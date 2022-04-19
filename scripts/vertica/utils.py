"""
Функции и утилиты для работы с Vertica
"""

import json
import logging
import logging.config
import os
from typing import Any, Collection, Dict, List, Optional

import pandas as pd
from vconnector.vertica_connector import VerticaConnector

from .config_files.config_vertica import SCHEMA, TABLE_PREFIX, table_foreign_keys
from .table_importer import TableImporter

# # Logging
logging.config.fileConfig('logging.conf')

# # Коннектор
vertica_configs = json.loads(os.getenv("VERTICA_CONFIGS"))
v_connector = VerticaConnector(user=os.getenv("VERTICA_USER_W"),
                               password=os.getenv("VERTICA_PASSWORD_W"),
                               database=os.getenv("VERTICA_DATABASE"),
                               vertica_configs=vertica_configs)


def column_constraint(column: Dict[str, Any]) -> str:
    """
    Создает Column-Constraint.
    https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/SQLReferenceManual/Statements/column-constraint.htm
    """
    references = column.get('REFERENCES')
    default = column.get('DEFAULT')

    constraint = ''
    constraint += ' NOT NULL' if column.get('NOT NULL') else ' NULL'
    constraint += ' UNIQUE' if column.get('UNIQUE') else ''
    constraint += ' PRIMARY KEY ENABLED' if column.get('PRIMARY KEY') else ''
    constraint += ' REFERENCES {}'.format(references) if references else ''
    constraint += ' DEFAULT {}'.format(default) if default else ''

    return constraint


def column_definition(column: Dict[str, Any]) -> str:
    """
    Создает Column-Definition.
    https://www.vertica.com/docs/9.2.x/HTML/Content/Authoring/SQLReferenceManual/Statements/column-definitionTable.htm
    """
    name = column.get('name')
    data_type = column.get('data_type')
    constraint = column_constraint(column)

    definition = '{} {}{}'.format(name, data_type, constraint)
    return definition


def make_default_id_column(method: str, table_name: str = None) -> Dict[str, Any]:
    """
    Создает столбец id для таблицы
    """
    if not table_name:
        table_name = TABLE_PREFIX + method

    column = {
        'name'       : 'id',
        'data_type'  : 'UUID',
        'NOT NULL'   : True,
        'UNIQUE'     : True,
        'PRIMARY KEY': True,
        'DEFAULT'    : "(public.MAPLOOKUP({}.__raw__, 'id'))::! UUID NOT NULL".format(table_name)
    }

    return column


def make_reference_column(table_name: str, schema: str = SCHEMA, table_prefix: str = TABLE_PREFIX) -> Dict[str, Any]:
    """
    Создает столбец с внешним ключом к другой таблице
    """
    column = {
        'name'      : '{}_id'.format(table_name),
        'data_type' : 'UUID',
        'NOT NULL'  : True,
        'UNIQUE'    : True,
        'REFERENCES': '{}.{}{}'.format(schema, table_prefix, table_name)
    }

    return column


def load_custom(df: pd.DataFrame, table_name: str, schema: Optional[str] = SCHEMA, table_prefix: Optional[str] = TABLE_PREFIX,
                skip_truncate: bool = False, table_type: str = 'FLEX TABLE', columns: Optional[List[Dict[str, Any]]] = None) -> None:
    """
    Загружает DataFrame df в таблицу table_name, с возможностью создания TABLE или FLEX TABLE и заранее определенными столбцами

    :param df: DataFrame
    :param table_name: имя таблицы
    :param schema: схема
    :param table_prefix: префикс имени таблицы
    :param skip_truncate: не очищать существующую таблицу
    :param table_type: TABLE или FLEX TABLE
    :param columns: список столбцов
    """
    full_table_name = '{}.{}{}'.format(schema, table_prefix, table_name)

    # templates
    sql = {
        'create'     : u"CREATE {} IF NOT EXISTS {} (".format(table_type, full_table_name),
        'truncate'   : u"TRUNCATE TABLE {}".format(full_table_name),
        'copy'       : u"COPY {} FROM STDIN PARSER FJSONPARSER()".format(full_table_name),
        'compute'    : u"SELECT COMPUTE_FLEXTABLE_KEYS_AND_BUILD_VIEW('{}')".format(full_table_name),
        'materialize': u"SELECT MATERIALIZE_FLEXTABLE_COLUMNS('{}')".format(full_table_name),
        'count'      : u"SELECT COUNT(*) AS cnt FROM {}".format(full_table_name),
    }

    if columns:
        sql['create'] += ', '.join([column_definition(c) for c in columns])
    sql['create'] += ')'

    with v_connector:
        cursor = v_connector.cnx.cursor()

        logging.info(sql['create'])
        cursor.execute(sql['create'])  # create

        if not skip_truncate:
            logging.info(sql['truncate'])
            cursor.execute(sql['truncate'])  # truncate

        logging.info(sql['copy'])
        cursor.copy(sql['copy'], df.to_json(orient='records', date_format='iso', date_unit='s'))  # copy

        if table_type == 'FLEX TABLE':
            logging.info(sql['compute'])
            cursor.execute(sql['compute'])  # compute keys and view

            logging.info(sql['materialize'])
            cursor.execute(sql['materialize'])  # materialize

        cursor.execute(sql['count'])  # count
        logging.info('Total items in {}: {}'.format(table_name, cursor.fetchone()[0]))
        cursor.close()


def load(df: pd.DataFrame, table_name: str, table_prefix: Optional[str] = TABLE_PREFIX) -> None:
    """
    Загружает DataFrame df в таблицу table_name при помощи коннектора TalentTech:
    сначала создает временную таблицу в STAGING_SCHEMA, копирует в нее json данные,
    затем удаляет оригинальную таблицу и переименовывает временную

    :param df: DataFrame
    :param table_name: имя таблицы
    :param table_prefix: префикс имени таблицы
    """
    os.environ["table_name"] = "{}{}".format(table_prefix, table_name)
    c = TableImporter(fields_names=df.columns)
    c.extract_full(df.to_json(orient='records', date_unit='s', date_format='iso'))


def get_tables(schema: str = SCHEMA, table_prefix: str = TABLE_PREFIX) -> List[str]:
    """
    Возвращает список таблиц в выбранной схеме и с выбранным префиксом
    """
    # templates
    sql = {
        'get': u"""
            SELECT DISTINCT table_name
            FROM all_tables
            WHERE schema_name = '{}'
                AND table_name ILIKE '{}%'
                AND table_name NOT ILIKE '%_keys'
                AND table_type != 'VIEW'
        """.format(schema, table_prefix),
    }

    with v_connector:
        cursor = v_connector.cnx.cursor()

        cursor.execute(sql['get'])  # count
        tables = [x[0] for x in cursor.fetchall()]
        logging.info('Total tables in DB: {}'.format(len(tables)))
        cursor.close()

    return tables


def get_columns(schema: str, table: str) -> List[str]:
    """
    Возвращает список столбцов в выбранной схеме и таблице
    """
    # templates
    sql = {
        'get_columns': u"""
            SELECT column_name
            FROM v_catalog.columns
            WHERE table_name = '{}'
              AND table_schema = '{}'
            ORDER BY ordinal_position
        """.format(table, schema),
    }

    with v_connector:
        cursor = v_connector.cnx.cursor()

        cursor.execute(sql['get_columns'])
        columns = [x[0] for x in cursor.fetchall()]
        logging.info('Total columns in {}.{}: {}'.format(schema, table, len(columns)))
        cursor.close()

    return columns


def get_constraints(schema: str, table: str) -> Dict[str, Dict[str, List[str]]]:
    """
    Возвращает список столбцов в выбранной схеме и таблице
    """
    # templates
    sql = {
        'get_constraints': u"""
            SELECT constraint_name,
                   column_name,
                   reference_table_schema,
                   reference_table_name,
                   reference_column_name
            FROM v_catalog.constraint_columns
            WHERE table_name = '{}'
              AND table_schema = '{}'
              AND constraint_type = 'f'
        """.format(table, schema),
    }

    with v_connector:
        cursor = v_connector.cnx.cursor()

        cursor.execute(sql['get_constraints'])
        constraints = cursor.fetchall()
        logging.info('Total foreign constraints in {}.{}: {}'.format(schema, table, len(constraints)))
        cursor.close()

        constraint_to_table = {}
        for constraint in constraints:
            constraint_name, column_name, reference_table_schema, reference_table_name, reference_column_name = constraint
            constraint_to_table[column_name] = {
                '{}.{}'.format(reference_table_schema, reference_table_name): [reference_column_name, constraint_name]
            }

    return constraint_to_table


def make_foreign_keys(schema: str = SCHEMA, table_prefix: str = TABLE_PREFIX) -> None:
    """
    Проходится по всем таблицам в схеме с префиксом table_prefix и при нахождении столбца с именем из table_foreign_keys, создает FOREIGN KEY
    """
    # templates
    # noinspection SyntaxError
    sql = {
        'add_fk'         : u"ALTER TABLE {SCHEMA}.{TABLE} ADD CONSTRAINT {TABLE}_{fk_table}_id_fk "
                           u"FOREIGN KEY ({fk}) REFERENCES {fk_schema}.{fk_table};",
        'set_search_path': u'set search_path = "{}"'.format(schema),
    }

    fk_to_table = {}
    for t, keys in table_foreign_keys.items():
        for k in keys:
            fk_to_table[k] = '{}{}'.format(table_prefix, t)

    tables = get_tables(schema, table_prefix)

    with v_connector:
        cursor = v_connector.cnx.cursor()

        for table in tables:
            columns = get_columns(schema=schema, table=table)
            constraints = get_constraints(schema=schema, table=table)

            for column in columns:
                if column in fk_to_table:
                    fk_table = fk_to_table[column]

                    if column in constraints:
                        fk_full_table_name = '{}.{}'.format(schema, fk_table)
                        if fk_full_table_name in constraints[column].keys():
                            logging.info('The foreign key {} has already been defined for relation {}'.format(column, fk_full_table_name))
                            continue

                    s = sql['add_fk'].format(SCHEMA=schema, TABLE=table, fk=column,
                                             fk_schema=schema, fk_table=fk_table)
                    try:
                        cursor.execute(s)
                        logging.info(s)
                    except Exception as e:
                        logging.error(s)
                        raise e

        cursor.close()


def get_table(table_name: str, schema: Optional[str] = SCHEMA, table_prefix: Optional[str] = TABLE_PREFIX, columns: List[str] = None) -> pd.DataFrame:
    """
    Загружает все данные из таблицы table_name и создает DataFrame

    :param table_name: имя таблицы
    :param schema: схема
    :param table_prefix: префикс имени таблицы
    :param columns: список столбцов
    """
    table = table_prefix + table_name
    full_table_name = '{}.{}'.format(schema, table)

    if not columns:
        columns = get_columns(schema=schema, table=table)
        # print(columns)

    # templates
    sql = {
        'get': u"SELECT {} FROM {}".format(', '.join(columns) if columns else '*', full_table_name),
    }

    with v_connector:
        cursor = v_connector.cnx.cursor()
        cursor.execute(sql['get'])
        data = cursor.fetchall()
        logging.info('Get {} rows from {}'.format(len(data), full_table_name))
        cursor.close()

    df = pd.DataFrame(data, columns=columns)

    return df


def make_link_table(populate_items: Collection, method: str, populate_method: str) -> (pd.DataFrame, list):
    """
    Создает таблицу связи между двумя сущностями
    """
    links = []
    for populate_item, ref_id in populate_items:
        links.append((populate_item.id, ref_id))

    columns = [make_reference_column(x) for x in (populate_method, method)]
    df = pd.DataFrame(links, columns=[x['name'] for x in columns])

    return df, columns
