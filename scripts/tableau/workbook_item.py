from tableauserverclient.models.exceptions import UnpopulatedPropertyError
from tableauserverclient.models.workbook_item import WorkbookItem


class WorkbookItemWithRevisions(WorkbookItem):
    def __init__(self, project_id, name=None, show_tabs=False):
        super(WorkbookItemWithRevisions, self).__init__(project_id, name, show_tabs)
        self._revisions = None

    @property
    def revisions(self):
        if self._revisions is None:
            error = "Workbook item must be populated with connections first."
            raise UnpopulatedPropertyError(error)
        return self._revisions()

    def _set_revisions(self, revisions):
        self._revisions = revisions
