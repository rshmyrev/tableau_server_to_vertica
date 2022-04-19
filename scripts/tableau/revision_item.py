# noinspection PyPep8Naming
import xml.etree.ElementTree as ET

from tableauserverclient.datetime_helpers import parse_datetime


class RevisionItem(object):
    """
    https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_revisions.htm
    """

    def __init__(self):
        self.revision_number = None
        self.published_at = None
        self.deleted = None
        self.current = None
        self.size_in_bytes = None
        self.publisher_id = None
        self.publisher_name = None

    def _set_values(self, revision_number, published_at, deleted, current, size_in_bytes, publisher_id, publisher_name):
        if revision_number is not None:
            self.revision_number = revision_number
        if published_at:
            self.published_at = published_at
        if deleted:
            self.deleted = deleted
        if current:
            self.current = current
        if size_in_bytes:
            self.size_in_bytes = size_in_bytes
        if publisher_id:
            self.publisher_id = publisher_id
        if publisher_name:
            self.publisher_name = publisher_name

    @classmethod
    def from_response(cls, resp, ns):
        all_revisions_items = list()
        parsed_response = ET.fromstring(resp)
        all_revisions_xml = parsed_response.findall('.//t:revision', namespaces=ns)
        for revision_xml in all_revisions_xml:
            (revision_number, published_at, deleted, current, size_in_bytes,
             publisher_id, publisher_name) = cls._parse_element(revision_xml, ns)

            workbook_item = cls()
            workbook_item._set_values(revision_number, published_at, deleted, current,
                                      size_in_bytes, publisher_id, publisher_name)
            all_revisions_items.append(workbook_item)
        return all_revisions_items

    @staticmethod
    def _parse_element(revision_xml, ns):
        revision_number = revision_xml.get('revisionNumber', None)
        if revision_number:
            revision_number = int(revision_number)
        published_at = parse_datetime(revision_xml.get('publishedAt', None))
        deleted = string_to_bool(revision_xml.get('deleted', None))
        current = string_to_bool(revision_xml.get('current', None))
        size_in_bytes = revision_xml.get('sizeInBytes', None)
        if size_in_bytes:
            size_in_bytes = int(size_in_bytes)

        publisher_id = None
        publisher_name = None
        publisher = revision_xml.find('.//t:publisher', namespaces=ns)
        if publisher is not None:
            publisher_id = publisher.get('id', None)
            publisher_name = publisher.get('name', None)

        return revision_number, published_at, deleted, current, size_in_bytes, publisher_id, publisher_name


# Used to convert string represented boolean to a boolean type
def string_to_bool(s):
    if not s:
        return None
    return s.lower() == 'true'
