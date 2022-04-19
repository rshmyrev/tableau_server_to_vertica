#!/usr/bin/env python
# coding: utf-8

# # Import
import logging.config
from typing import Collection

import pandas as pd
import tableauserverclient as TSC

from helper import attributes
from tableau.utils import download, download_png, get_items, get_populate_items, make_df, make_project_path
from vertica.utils import load, load_custom, make_foreign_keys, make_link_table

# # Logging

logging.config.fileConfig('logging.conf')
logging.info('Start ETL Tableau')

# # Variables
DOWNLOAD_PNG = False
DOWNLOAD_WORKBOOK = False
DOWNLOAD_DATASOURCE = False
DOWNLOAD_WITHOUT_EXTRACT = True


# # Functions

def et(method: str) -> (TSC.server.endpoint.endpoint.QuerysetEndpoint, Collection, pd.DataFrame):
    """
    Extract and transform для обычного кейса: забрать все данные для одного endpoint и сделать таблицу
    """
    items, endpoint = get_items(method)
    df = make_df(items, attributes[method])
    return endpoint, items, df


def etl(method: str) -> (TSC.server.endpoint.endpoint.QuerysetEndpoint, Collection, pd.DataFrame):
    """
    ETL для обычного кейса: забрать все данные для одного endpoint и загрузить их в таблицу с соответствующим названием.
    """
    endpoint, items, df = et(method)
    load(df, table_name=method)
    return endpoint, items, df


def make_df_for_populated(populate_method: str) -> pd.DataFrame:
    populate_items = get_populate_items(items, endpoint, populate_method)
    return make_df([x[0] for x in populate_items], attributes[populate_method])


def make_df_for_populated_with_id(populate_method: str, column_id: str = 'workbook_id') -> pd.DataFrame:
    populate_items = get_populate_items(items, endpoint, populate_method)
    df = make_df([x[0] for x in populate_items], attributes[populate_method])
    df[column_id] = [x[1] for x in populate_items]
    return df


# # Workbooks

method = 'workbooks'
endpoint, items, _ = etl(method)

# ## Download

# ### Download preview_image

if DOWNLOAD_PNG:
    for i in items:
        endpoint.populate_preview_image(i)
        download_png(i)

# ### Download workbook

if DOWNLOAD_WORKBOOK:
    for i in items:
        try:
            download(i, method, no_extract=DOWNLOAD_WITHOUT_EXTRACT)
        except:
            logging.error('Error while save {}'.format(i.name))

# ## Revisions

populate_method = 'revisions'
df = make_df_for_populated_with_id(populate_method, column_id='workbook_id')
load(df, table_name='{}_{}'.format(method, populate_method))

# ## Connections in Workbooks

populate_method = 'connections'
df = make_df_for_populated_with_id(populate_method, column_id='workbook_id')
load(df, table_name=populate_method)

# ## Views in Workbooks

populate_method = 'views'
df = make_df_for_populated(populate_method)
df['position'] = df.groupby('workbook_id').cumcount() + 1
del df['owner_id'], df['project_id'], df['tags']
load(df, table_name=populate_method)

# # Datasources

method = 'datasources'
endpoint, items, _ = etl(method)

# ## Download datasource

if DOWNLOAD_DATASOURCE:
    for i in items:
        try:
            download(i, method, no_extract=DOWNLOAD_WITHOUT_EXTRACT)
        except:
            logging.error('Error while save {}'.format(i.name))

# ## Connections in Datasources

populate_method = 'connections'
df = make_df_for_populated_with_id(populate_method, column_id='datasource_id')
load_custom(df, table_name=populate_method, skip_truncate=True, table_type='TABLE')

# # Projects

method = 'projects'
_, items, df = et(method)
df['path'] = df['id'].apply(lambda x: '/'.join(make_project_path(items)[x]))
load(df, table_name=method)

# # Users

method = 'users'
endpoint, items, _ = etl(method)

# ## Workbooks in Users

populate_method = 'workbooks'
populate_items = get_populate_items(items, endpoint, populate_method)
df, columns = make_link_table(populate_items, method, populate_method)
load(df, table_name='{}_{}'.format(method, populate_method))

# # Groups

method = 'groups'
endpoint, items, _ = etl(method)

# ## Users in group

populate_method = 'users'
populate_items = get_populate_items(items, endpoint, populate_method)
df, _ = make_link_table(populate_items, method, populate_method)
load(df, table_name='{}_{}'.format(method, populate_method))

# # Subscriptions

method = 'subscriptions'
_, _, df = et(method)
df['target_type'] = df['target'].apply(lambda x: getattr(x, 'type'))
df['target_id'] = df['target'].apply(lambda x: getattr(x, 'id'))
del df['target']
load(df, table_name=method)

# # Schedules

method = 'schedules'
_ = etl(method)

# # Make foreign keys

make_foreign_keys()
