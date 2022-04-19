CREATE TABLE netology_temp.tableau_connections
(
    id              UUID,
    connection_type VARCHAR,
    datasource_id   UUID,
    datasource_name VARCHAR(500),
    embed_password  BOOLEAN,
    password        VARCHAR,
    server_address  VARCHAR,
    server_port     INTEGER,
    username        VARCHAR,
    workbook_id     UUID,
    CONSTRAINT tableau_connections_pk PRIMARY KEY (id) ENABLED
);
