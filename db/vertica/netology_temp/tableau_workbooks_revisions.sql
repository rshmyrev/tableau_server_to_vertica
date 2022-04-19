CREATE TABLE netology_temp.tableau_workbooks_revisions
(
    workbook_id     UUID,
    revision_number INTEGER,
    published_at    TIMESTAMP,
    publisher_id    UUID,
    publisher_name  VARCHAR,
    size_in_bytes   INTEGER,
    current         BOOLEAN,
    deleted         BOOLEAN,
    CONSTRAINT tableau_users_pk PRIMARY KEY (workbook_id, revision_number) ENABLED
);
