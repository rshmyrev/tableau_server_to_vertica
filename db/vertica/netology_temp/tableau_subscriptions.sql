CREATE TABLE netology_temp.tableau_subscriptions
(
    id          UUID,
    schedule_id UUID,
    subject     VARCHAR(500),
    target_id   UUID,
    target_type VARCHAR,
    user_id     UUID,
    CONSTRAINT tableau_subscriptions_pk PRIMARY KEY (id) ENABLED
);
