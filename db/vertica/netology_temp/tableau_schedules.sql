CREATE TABLE netology_temp.tableau_schedules
(
    id              UUID,
    execution_order VARCHAR,
    interval_item   VARCHAR,
    name            VARCHAR(500),
    priority        INTEGER,
    schedule_type   VARCHAR,
    CONSTRAINT tableau_schedules_pk PRIMARY KEY (id) ENABLED
);
