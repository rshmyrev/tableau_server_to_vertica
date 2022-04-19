import logging

from tableauserverclient.server import PaginationItem
from tableauserverclient.server.endpoint.endpoint import api
from tableauserverclient.server.endpoint.exceptions import MissingRequiredFieldError
from tableauserverclient.server.endpoint.workbooks_endpoint import Workbooks

from .revision_item import RevisionItem
from .workbook_item import WorkbookItemWithRevisions

logger = logging.getLogger('tableau.endpoint.workbooks')


class WorkbooksWithRevisions(Workbooks):
    def __init__(self, parent_srv):
        super(WorkbooksWithRevisions, self).__init__(parent_srv)

    # Get all revisions of workbook
    @api(version="2.0")
    def populate_revisions(self, workbook_item):
        if not workbook_item.id:
            error = "Workbook item missing ID. Workbook must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        def revision_fetcher():
            return self._get_workbook_revisions(workbook_item)

        workbook_item._set_revisions(revision_fetcher)
        logger.info('Populated revisions for workbook (ID: {0})'.format(workbook_item.id))

    def _get_workbook_revisions(self, workbook_item, req_options=None):
        """
        GET /api/api-version/sites/site-id/workbooks/workbook-id/revision
        """
        url = "{0}/{1}/revisions".format(self.baseurl, workbook_item.id)
        server_response = self.get_request(url, req_options)
        connections = RevisionItem.from_response(server_response.content, self.parent_srv.namespace)
        return connections

    # Get all workbooks on site
    @api(version="2.0")
    def get(self, req_options=None):
        logger.info('Querying all workbooks on site')
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(
            server_response.content, self.parent_srv.namespace)
        all_workbook_items = WorkbookItemWithRevisions.from_response(
            server_response.content, self.parent_srv.namespace)
        return all_workbook_items, pagination_item

    # Get 1 workbook
    @api(version="2.0")
    def get_by_id(self, workbook_id):
        if not workbook_id:
            error = "Workbook ID undefined."
            raise ValueError(error)
        logger.info('Querying single workbook (ID: {0})'.format(workbook_id))
        url = "{0}/{1}".format(self.baseurl, workbook_id)
        server_response = self.get_request(url)
        return WorkbookItemWithRevisions.from_response(server_response.content, self.parent_srv.namespace)[0]
