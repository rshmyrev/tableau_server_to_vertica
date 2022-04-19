CREATE TABLE netology_temp.tableau_datasources
(
    id                     UUID,
    ask_data_enablement    VARCHAR,
    certification_note     VARCHAR,
    certified              BOOLEAN,
    content_url            VARCHAR,
    created_at             TIMESTAMP,
    datasource_type        VARCHAR,
    description            VARCHAR,
    encrypt_extracts       BOOLEAN,
    has_extracts           VARCHAR,
    name                   VARCHAR(500),
    owner_id               UUID,
    project_id             UUID,
    project_name           VARCHAR,
    tags                   VARBINARY(1000),
    updated_at             TIMESTAMP,
    use_remote_query_agent BOOLEAN,
    webpage_url            VARCHAR,
    CONSTRAINT tableau_datasources_pk PRIMARY KEY (id) ENABLED
);
