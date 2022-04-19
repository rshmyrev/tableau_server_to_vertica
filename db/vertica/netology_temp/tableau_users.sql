CREATE TABLE netology_temp.tableau_users
(
    id                    UUID,
    auth_setting          VARCHAR,
    domain_name           VARCHAR,
    email                 VARCHAR,
    external_auth_user_id VARCHAR,
    fullname              VARCHAR,
    last_login            TIMESTAMP,
    name                  VARCHAR,
    site_role             VARCHAR,
    tag_name              VARCHAR,
    CONSTRAINT tableau_users_pk PRIMARY KEY (id) ENABLED
);
