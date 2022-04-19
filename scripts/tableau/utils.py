"""
Функции и утилиты для работы с Tableau Server
"""
import logging
import os
from collections import defaultdict
from pathlib import Path
from typing import Collection, Dict, List, Tuple, Union

import pandas as pd
import tableauserverclient as TSC

from .workbooks_endpoint import WorkbooksWithRevisions

tableau_auth = TSC.PersonalAccessTokenAuth(os.getenv('TABLEAU_TOKEN_NAME'),
                                           os.getenv('TABLEAU_TOKEN_VALUE'),
                                           os.getenv('TABLEAU_SITENAME'))
server = TSC.Server(os.getenv('TABLEAU_SERVER_URL'))
request_options = TSC.RequestOptions(pagesize=1000)

# Sign in
server.auth.sign_in(tableau_auth)
server.use_server_version()

# mapping
endpoints = {
    'workbooks'    : WorkbooksWithRevisions(server),
    'views'        : server.views,
    'datasources'  : server.datasources,
    'projects'     : server.projects,
    'users'        : server.users,
    'groups'       : server.groups,
    'subscriptions': server.subscriptions,
    'schedules'    : server.schedules,
    'sites'        : server.sites,
    'jobs'         : server.jobs,
    'server_info'  : server.server_info,
}


def get_items(method: str) -> \
        Tuple[Collection[Union[TSC.DatasourceItem, TSC.GroupItem, TSC.ProjectItem,
                               TSC.ScheduleItem, TSC.SubscriptionItem,
                               TSC.UserItem, TSC.ViewItem, TSC.WorkbookItem]],
              TSC.server.endpoint.endpoint.QuerysetEndpoint]:
    """
    Забирает все сущности указанного method. Вызывает функцию get
    """
    endpoint = endpoints.get(method)
    _, pagination_item = endpoint.get(TSC.RequestOptions())
    items = list(TSC.Pager(endpoint, request_options))
    logging.info("There are {} {} on site. Get {}".format(
        pagination_item.total_available, method, len(items)))
    return items, endpoint


def get_populate_items(items: Collection[Union[TSC.DatasourceItem, TSC.GroupItem, TSC.UserItem, TSC.ViewItem, TSC.WorkbookItem]],
                       endpoint: TSC.server.endpoint.endpoint.QuerysetEndpoint,
                       populate_method: str) -> List[Tuple[object, Union[int, str]]]:
    """
    Вызывает функцию populate_method для каждого объекта в items
    https://tableau.github.io/server-client-python/docs/populate-connections-views
    """
    # Определяем функцию вызова в зависимости от endpoint
    populate_func = getattr(endpoint, 'populate_{}'.format(populate_method))

    populate_items = []
    for i in items:
        populate_func(i)
        for x in getattr(i, populate_method):
            populate_items.append((x, i.id))
    logging.info("There are {} {} in {} {}".format(
        len(populate_items), populate_method, len(items), endpoint.baseurl.rsplit('/', 1)[-1]))

    return populate_items


def download(obj: Union[TSC.models.workbook_item.WorkbookItem, TSC.models.datasource_item.DatasourceItem],
             method: str = 'workbooks', no_extract: bool = False) -> None:
    """
    Скачать workbook или datasource в папку data.
    https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#download_workbook
    https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#download_data_source

    :param obj: workbook или datasource
    :param method: workbooks/datasources
    :param no_extract: Specifies whether to download the file without the extract
    """
    name = obj.name.replace('/', '|').strip()
    path = Path.cwd().parent / 'data' / method / name
    path.mkdir(parents=True, exist_ok=True)

    saved_path = None
    endpoint = endpoints.get(method)
    if method == 'workbooks':
        saved_path = endpoint.download(obj.id, filepath=path, no_extract=no_extract)
    elif method == 'datasources':
        saved_path = endpoint.download(obj.id, filepath=path, include_extract=not no_extract)

    saved_path = Path(saved_path)
    suffix = saved_path.suffix
    new_path = path.with_suffix(suffix)

    saved_path.rename(new_path)
    logging.info('Saved {}'.format(new_path))
    path.rmdir()


def download_png(obj: TSC.models.workbook_item.WorkbookItem) -> None:
    """
    Скачивает preview_image
    """
    path = Path.cwd().parent / 'data' / 'preview_image'
    path.mkdir(parents=True, exist_ok=True)

    filename = path / '{}.png'.format(obj.id)
    with open(filename, 'wb') as f:
        f.write(obj.preview_image)
    logging.info('Saved {}'.format(filename))


def make_df(objs: Collection, attrs: Collection[str]) -> pd.DataFrame:
    """
    Создает Pandas DataFrame из объектов objs со столбцами, описанными в attrs
    """

    def replace_url(url: str) -> str:
        """
        Удаляет начальную часть ссылки Tableau Server
        """
        return url.replace('http://tableau4/#/site/NetologyGroup/', '')

    data = [[getattr(obj, a) for a in attrs] for obj in objs]
    df = pd.DataFrame(data, columns=attrs)
    if 'webpage_url' in df:
        df['webpage_url'] = df['webpage_url'].apply(replace_url)

    return df


def make_project_path(items: Collection[TSC.ProjectItem]) -> Dict[Union[str, int], Collection[str]]:
    """
    Возвращает полный путь до проекта
    """
    project_dict = {}
    for i in items:
        project_dict[i.id] = i

    project_path = defaultdict(list)
    for i in items:
        parent_id = i.parent_id
        project_path[i.id].insert(0, i.name)
        while parent_id:
            parent = project_dict[parent_id]
            project_path[i.id].insert(0, parent.name)
            parent_id = parent.parent_id

    for i in items:
        path = project_path[i.id]
        i.path = path

    del project_dict
    return project_path
