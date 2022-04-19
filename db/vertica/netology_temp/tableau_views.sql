CREATE TABLE netology_temp.tableau_views
(
    id          UUID,
    content_url VARCHAR(500),
    created_at  TIMESTAMP,
    name        VARCHAR(500),
    owner_id    UUID,
    project_id  UUID,
    sheet_type  VARCHAR,
    tags        VARBINARY(1000),
    updated_at  TIMESTAMP,
    workbook_id UUID,
    position    INT,
    CONSTRAINT tableau_views_pk PRIMARY KEY (id) ENABLED
);
