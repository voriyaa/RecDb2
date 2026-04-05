-- recdb2: 0.1.3 -> 0.1.4

--1. Predictions table
CREATE TABLE IF NOT EXISTS recdb2_predictions (
    model_id     bigint NOT NULL REFERENCES recdb2_models(id) ON DELETE CASCADE,
    item_id      bigint NOT NULL,
    score        double precision NOT NULL,
    PRIMARY KEY (model_id, item_id)
);

CREATE INDEX IF NOT EXISTS idx_recdb2_predictions_model_score
    ON recdb2_predictions (model_id, score DESC);


--2. Update recommenders view (add config_json)
CREATE OR REPLACE VIEW recdb2_recommenders AS
SELECT id, name, algorithm, state, created_at, updated_at, last_error, config_json
FROM recdb2_models
ORDER BY id;

--3. Training status view
CREATE OR REPLACE VIEW recdb2_training_status AS
SELECT
    m.id,
    m.name,
    m.algorithm,
    m.state,
    m.updated_at AS last_state_change,
    m.last_error,
    (SELECT count(*) FROM recdb2_predictions p where p.model_id = m.id) AS predictions_count
FROM recdb2_models m
ORDER BY m.id;

--4. New functions

-- train
CREATE OR REPLACE FUNCTION recdb2_train(model_name text)
RETURNS text
AS 'MODULE_PATHNAME', 'recdb2_train'
LANGUAGE C STRICT;

-- recommend
CREATE OR REPLACE FUNCTION recdb2_recommend(
    model_name text,
    target_user_id bigint,
    top_n integer DEFAULT 10
)
RETURNS TABLE(item_id bigint, score double precision)
AS 'MODULE_PATHNAME', 'recdb2_recommend'
LANGUAGE C STRICT;

-- retrain
CREATE OR REPLACE FUNCTION recdb2_retrain(model_name text)
RETURNS text
AS 'MODULE_PATHNAME', 'recdb2_retrain'
LANGUAGE C STRICT;

-- drop
CREATE OR REPLACE FUNCTION recdb2_drop_recommender(model_name text)
RETURNS text
AS 'MODULE_PATHNAME', 'recdb2_drop_recommender'
LANGUAGE C STRICT;
