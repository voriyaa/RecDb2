CREATE TABLE recdb2_models (
    id           bigserial PRIMARY KEY,
    name         text NOT NULL UNIQUE,
    algorithm    text NOT NULL,
    state        text NOT NULL DEFAULT 'created',
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    last_error   text,
    config_json  jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE OR REPLACE VIEW recdb2_recommenders AS
SELECT id, name, algorithm, state, created_at, updated_at, last_error
FROM recdb2_models
ORDER BY id;
