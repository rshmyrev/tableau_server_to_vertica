CREATE TABLE netology_temp.tableau_projects
(
    id                  UUID,
    content_permissions VARCHAR,
    description         VARCHAR(500),
    name                VARCHAR(500),
    parent_id           UUID,
    path                VARCHAR(500),
    CONSTRAINT tableau_projects_pk PRIMARY KEY (id) ENABLED
);
