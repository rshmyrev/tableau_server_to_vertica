SCHEMA = 'netology_temp'
STAGING_SCHEMA = 'netology_staging'
TABLE_PREFIX = 'tableau_'

table_foreign_keys = {
    'datasources': ['datasource_id', ],
    'workbooks'  : ['workbook_id', 'workbooks_id'],
    'users'      : ['user_id', 'users_id', 'owner_id', 'publisher_id'],
    'projects'   : ['project_id', 'parent_id'],
    'groups'     : ['groups_id', ],
    'schedules'  : ['schedule_id', ],
}
