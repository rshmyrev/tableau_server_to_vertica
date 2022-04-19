CREATE TABLE netology_temp.tableau_groups
(
    id                UUID,
    domain_name       VARCHAR,
    license_mode      VARCHAR,
    minimum_site_role VARCHAR,
    name              VARCHAR,
    tag_name          VARCHAR,
    CONSTRAINT tableau_groups_pk PRIMARY KEY (id) ENABLED
);
