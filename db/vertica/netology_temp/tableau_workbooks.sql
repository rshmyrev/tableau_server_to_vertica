CREATE TABLE IF NOT EXISTS netology_temp.tableau_workbooks
(
    id           UUID,
    content_url  VARCHAR,
    created_at   TIMESTAMP,
    description  VARCHAR(10000),
    name         VARCHAR(500),
    owner_id     UUID,
    project_id   UUID,
    project_name VARCHAR(500),
    show_tabs    BOOLEAN,
    size         INTEGER,
    tags         VARBINARY(1000),
    updated_at   TIMESTAMP,
    webpage_url  VARCHAR(500),
    CONSTRAINT tableau_workbooks_pk PRIMARY KEY (id) ENABLED
);
