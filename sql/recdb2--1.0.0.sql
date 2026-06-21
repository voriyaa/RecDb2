CREATE TABLE recdb2_models (
    id            bigserial PRIMARY KEY,
    name          text NOT NULL UNIQUE,
    algorithm     text NOT NULL,
    state         text NOT NULL DEFAULT 'created',
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    last_error    text,
    config_json   jsonb NOT NULL DEFAULT '{}'::jsonb,
    learned_state jsonb
);

CREATE TABLE recdb2_predictions (
    model_id  bigint NOT NULL REFERENCES recdb2_models(id) ON DELETE CASCADE,
    item_id   bigint NOT NULL,
    score     double precision NOT NULL,
    PRIMARY KEY (model_id, item_id)
);

CREATE INDEX idx_recdb2_predictions_model_score
    ON recdb2_predictions (model_id, score DESC);

CREATE VIEW recdb2_recommenders AS
SELECT id, name, algorithm, state, created_at, updated_at, last_error, config_json, learned_state
FROM recdb2_models ORDER BY id;

CREATE VIEW recdb2_training_status AS
SELECT m.id, m.name, m.algorithm, m.state,
       m.updated_at AS last_state_change, m.last_error,
       (SELECT count(*) FROM recdb2_predictions p WHERE p.model_id = m.id) AS predictions_count
FROM recdb2_models m ORDER BY m.id;

CREATE FUNCTION recdb2_hello() RETURNS text
AS 'MODULE_PATHNAME', 'recdb2_hello' LANGUAGE C STRICT;

CREATE FUNCTION recdb2_spi_ping() RETURNS text
AS 'MODULE_PATHNAME', 'recdb2_spi_ping' LANGUAGE C STRICT;

CREATE FUNCTION recdb2_create_recommender_core(name text, algorithm text, config_json_text text)
RETURNS bigint
AS 'MODULE_PATHNAME', 'recdb2_create_recommender' LANGUAGE C STRICT;

CREATE FUNCTION recdb2_train(model_name text) RETURNS text
AS 'MODULE_PATHNAME', 'recdb2_train' LANGUAGE C STRICT;

CREATE FUNCTION recdb2_recommend(model_name text, target_user_id bigint, top_n integer DEFAULT 10)
RETURNS TABLE(item_id bigint, score double precision)
AS 'MODULE_PATHNAME', 'recdb2_recommend' LANGUAGE C STRICT;

CREATE FUNCTION recdb2_explain(model_name text, target_user_id bigint, target_item_id bigint)
RETURNS TABLE(kind text, label text, contribution double precision, details jsonb)
AS 'MODULE_PATHNAME', 'recdb2_explain' LANGUAGE C STRICT;

CREATE FUNCTION recdb2_introspect(model_name text)
RETURNS TABLE(kind text, label text, contribution double precision, details jsonb)
AS 'MODULE_PATHNAME', 'recdb2_introspect' LANGUAGE C STRICT;

CREATE FUNCTION recdb2_retrain(model_name text) RETURNS text
AS 'MODULE_PATHNAME', 'recdb2_retrain' LANGUAGE C STRICT;

CREATE FUNCTION recdb2_drop_recommender(model_name text) RETURNS text
AS 'MODULE_PATHNAME', 'recdb2_drop_recommender' LANGUAGE C STRICT;

-- PARALLEL SAFE: функция read-only, без побочных эффектов.
-- Per-backend cache (model_id, updated_at) безопасен в parallel workers:
-- каждый worker — отдельный backend-процесс со своим cache,
-- инвалидация через updated_at работает прозрачно.
CREATE FUNCTION recdb2_score(model_name text, target_user_id bigint, target_item_id bigint)
RETURNS double precision
AS 'MODULE_PATHNAME', 'recdb2_score' LANGUAGE C STRICT PARALLEL SAFE;

CREATE FUNCTION recdb2_evaluate(
    model_name text,
    k int DEFAULT 10,
    rating_threshold double precision DEFAULT 4.0,
    split_modulus int DEFAULT 5
) RETURNS TABLE(metric text, value double precision)
LANGUAGE plpgsql AS $func$
DECLARE
    cfg jsonb;
    algo text;
    model_id_v bigint;
    interactions text;
    user_col_v text;
    item_col_v text;
    rating_col_v text;
    sql text;
BEGIN
    SELECT id, algorithm, config_json INTO model_id_v, algo, cfg
    FROM recdb2_models WHERE name = model_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'recdb2: model % not found', model_name;
    END IF;

    IF cfg ? 'interactions' THEN
        interactions := cfg->'interactions'->>'table';
        user_col_v := cfg->'interactions'->>'user_col';
        item_col_v := cfg->'interactions'->>'item_col';
        rating_col_v := cfg->'interactions'->>'rating_col';
    ELSE
        interactions := cfg->>'ratings_table';
        user_col_v := cfg->>'user_col';
        item_col_v := cfg->>'item_col';
        rating_col_v := cfg->>'rating_col';
    END IF;

    sql := format($q$
        WITH test_set AS (
            SELECT %I AS user_id, %I AS item_id, %I::double precision AS rating
            FROM %I
            WHERE abs(hashtextextended(%I::text || ':' || %I::text, 0)) %% $1 = 0
        ),
        scored AS (
            SELECT t.user_id, t.item_id,
                   (t.rating >= $2)::int AS relevant,
                   recdb2_score(%L, t.user_id, t.item_id) AS score
            FROM test_set t
        ),
        ranked AS (
            SELECT user_id, item_id, relevant, score,
                   row_number() OVER (PARTITION BY user_id ORDER BY score DESC, item_id) AS rk
            FROM scored
        ),
        per_user_total AS (
            SELECT user_id, sum(relevant) AS total_relevant
            FROM scored GROUP BY user_id
        ),
        topk AS (
            SELECT user_id, item_id, relevant, rk
            FROM ranked WHERE rk <= $3
        ),
        per_user AS (
            SELECT t.user_id, pu.total_relevant,
                   sum(t.relevant)::double precision / $3 AS p,
                   sum(t.relevant)::double precision / NULLIF(pu.total_relevant, 0) AS r,
                   sum(CASE WHEN t.relevant = 1
                            THEN 1.0 / log(2.0, (t.rk + 1)::numeric)
                            ELSE 0 END)::double precision /
                       NULLIF((SELECT sum(1.0 / log(2.0, (s + 1)::numeric))
                               FROM generate_series(1, LEAST($3, pu.total_relevant)::int) AS s)::double precision, 0)
                       AS ndcg
            FROM topk t JOIN per_user_total pu ON pu.user_id = t.user_id
            WHERE pu.total_relevant > 0
            GROUP BY t.user_id, pu.total_relevant
        )
        SELECT 'precision_at_k'::text, avg(p) FROM per_user
        UNION ALL SELECT 'recall_at_k', avg(r) FROM per_user
        UNION ALL SELECT 'ndcg_at_k', avg(ndcg) FROM per_user
        UNION ALL SELECT 'n_users_evaluated', count(*)::double precision FROM per_user
        UNION ALL SELECT 'n_test_interactions',
                          (SELECT count(*)::double precision FROM scored)
    $q$, user_col_v, item_col_v, rating_col_v, interactions, user_col_v, item_col_v, model_name);

    RETURN QUERY EXECUTE sql USING split_modulus, rating_threshold, k;
END;
$func$;

CREATE FUNCTION recdb2_compare(
    model_names text[],
    k int DEFAULT 10,
    rating_threshold double precision DEFAULT 4.0
) RETURNS TABLE(model text, precision_at_k double precision, recall_at_k double precision,
                ndcg_at_k double precision, n_users int)
LANGUAGE plpgsql AS $func$
DECLARE
    nm text;
    p double precision;
    r double precision;
    n double precision;
    nu double precision;
BEGIN
    FOREACH nm IN ARRAY model_names LOOP
        SELECT max(CASE WHEN m='precision_at_k' THEN v END),
               max(CASE WHEN m='recall_at_k' THEN v END),
               max(CASE WHEN m='ndcg_at_k' THEN v END),
               max(CASE WHEN m='n_users_evaluated' THEN v END)
        INTO p, r, n, nu
        FROM recdb2_evaluate(nm, k, rating_threshold) AS x(m, v);
        model := nm; precision_at_k := p; recall_at_k := r; ndcg_at_k := n;
        n_users := nu::int;
        RETURN NEXT;
    END LOOP;
END;
$func$;

-- Расширенная оценка: long-tail NDCG, catalog coverage, ECE (calibration).
-- Эти метрики бьют именно в сильные стороны recdb2 vs popularity.
CREATE FUNCTION recdb2_evaluate_full(
    model_name text, k int DEFAULT 10,
    rating_threshold double precision DEFAULT 4.0,
    split_modulus int DEFAULT 5,
    long_tail_max int DEFAULT 50
) RETURNS TABLE(metric text, value double precision)
LANGUAGE plpgsql AS $func$
DECLARE
    cfg jsonb;
    interactions text;
    user_col_v text; item_col_v text; rating_col_v text;
    sql text;
BEGIN
    SELECT config_json INTO cfg FROM recdb2_models WHERE name = model_name;
    IF NOT FOUND THEN RAISE EXCEPTION 'recdb2: model % not found', model_name; END IF;
    IF cfg ? 'interactions' THEN
        interactions := cfg->'interactions'->>'table';
        user_col_v := cfg->'interactions'->>'user_col';
        item_col_v := cfg->'interactions'->>'item_col';
        rating_col_v := cfg->'interactions'->>'rating_col';
    ELSE
        interactions := cfg->>'ratings_table';
        user_col_v := cfg->>'user_col';
        item_col_v := cfg->>'item_col';
        rating_col_v := cfg->>'rating_col';
    END IF;

    sql := format($q$
        WITH item_counts AS (
            SELECT %I AS iid, count(*) AS n FROM %I GROUP BY %I
        ),
        test_set AS (
            SELECT %I AS user_id, %I AS item_id, %I::double precision AS rating
            FROM %I
            WHERE abs(hashtextextended(%I::text || ':' || %I::text, 0)) %% $1 = 0
        ),
        scored AS (
            SELECT t.user_id, t.item_id,
                   (t.rating >= $2)::int AS relevant,
                   GREATEST(0.0, LEAST(1.0, recdb2_score(%L, t.user_id, t.item_id))) AS score,
                   ic.n AS item_n
            FROM test_set t JOIN item_counts ic ON ic.iid = t.item_id
        ),
        ranked AS (
            SELECT user_id, item_id, relevant, score, item_n,
                   row_number() OVER (PARTITION BY user_id ORDER BY score DESC, item_id) AS rk
            FROM scored
        ),
        topk AS (SELECT * FROM ranked WHERE rk <= $3),
        lt_total AS (
            SELECT user_id, sum(CASE WHEN item_n < $4 AND relevant=1 THEN 1 ELSE 0 END) AS n_lt
            FROM scored GROUP BY user_id
        ),
        lt_ndcg AS (
            SELECT t.user_id,
                   sum(CASE WHEN t.relevant=1 AND t.item_n < $4
                            THEN 1.0 / log(2.0, (t.rk + 1)::numeric) ELSE 0 END)::double precision /
                   NULLIF((SELECT sum(1.0/log(2.0,(s+1)::numeric))
                           FROM generate_series(1, LEAST($3, pu.n_lt)::int) AS s)::double precision, 0)
                   AS ndcg
            FROM topk t JOIN lt_total pu ON pu.user_id = t.user_id
            WHERE pu.n_lt > 0 GROUP BY t.user_id, pu.n_lt
        ),
        bins AS (
            SELECT LEAST(9, floor(score*10)::int) AS bin,
                   avg(score)::double precision AS conf,
                   avg(relevant)::double precision AS acc,
                   count(*) AS n
            FROM scored GROUP BY bin
        )
        SELECT 'long_tail_ndcg_at_k'::text, COALESCE(avg(ndcg), 0) FROM lt_ndcg
        UNION ALL
        SELECT 'catalog_coverage_at_k',
               count(DISTINCT item_id)::double precision /
               NULLIF((SELECT count(DISTINCT %I)::double precision FROM %I), 0)
        FROM topk
        UNION ALL
        SELECT 'ece',
               sum(n * abs(conf - acc))::double precision /
               NULLIF((SELECT sum(n) FROM bins)::double precision, 0)
        FROM bins
    $q$,
    item_col_v, interactions, item_col_v,
    user_col_v, item_col_v, rating_col_v, interactions, user_col_v, item_col_v,
    model_name,
    item_col_v, interactions);

    RETURN QUERY EXECUTE sql USING split_modulus, rating_threshold, k, long_tail_max;
END;
$func$;

CREATE FUNCTION recdb2_compare_full(
    model_names text[], k int DEFAULT 10, rating_threshold double precision DEFAULT 4.0,
    long_tail_max int DEFAULT 50
) RETURNS TABLE(
    model text,
    ndcg_at_k double precision,
    long_tail_ndcg double precision,
    coverage double precision,
    ece double precision
)
LANGUAGE plpgsql AS $func$
DECLARE nm text; n1 double precision; n2 double precision; c double precision; e double precision;
BEGIN
    FOREACH nm IN ARRAY model_names LOOP
        SELECT max(CASE WHEN m='ndcg_at_k' THEN v END) INTO n1
        FROM recdb2_evaluate(nm, k, rating_threshold) AS x(m, v);
        SELECT max(CASE WHEN m='long_tail_ndcg_at_k' THEN v END),
               max(CASE WHEN m='catalog_coverage_at_k' THEN v END),
               max(CASE WHEN m='ece' THEN v END)
        INTO n2, c, e
        FROM recdb2_evaluate_full(nm, k, rating_threshold, 5, long_tail_max) AS x(m, v);
        model := nm; ndcg_at_k := n1; long_tail_ndcg := n2; coverage := c; ece := e;
        RETURN NEXT;
    END LOOP;
END;
$func$;

CREATE FUNCTION recdb2_dataset_info(
    interactions_table text, user_col text, item_col text, rating_col text
) RETURNS TABLE(metric text, value double precision)
LANGUAGE plpgsql AS $$
DECLARE sql text;
BEGIN
    sql := format('SELECT count(*)::double precision FROM %I', interactions_table);
    metric := 'n_interactions'; EXECUTE sql INTO value; RETURN NEXT;

    sql := format('SELECT count(DISTINCT %I)::double precision FROM %I', user_col, interactions_table);
    metric := 'n_users'; EXECUTE sql INTO value; RETURN NEXT;

    sql := format('SELECT count(DISTINCT %I)::double precision FROM %I', item_col, interactions_table);
    metric := 'n_items'; EXECUTE sql INTO value; RETURN NEXT;

    sql := format('SELECT min(%I)::double precision FROM %I', rating_col, interactions_table);
    metric := 'rating_min'; EXECUTE sql INTO value; RETURN NEXT;

    sql := format('SELECT max(%I)::double precision FROM %I', rating_col, interactions_table);
    metric := 'rating_max'; EXECUTE sql INTO value; RETURN NEXT;

    sql := format('SELECT avg(%I)::double precision FROM %I', rating_col, interactions_table);
    metric := 'rating_mean'; EXECUTE sql INTO value; RETURN NEXT;

    sql := format('SELECT stddev(%I)::double precision FROM %I', rating_col, interactions_table);
    metric := 'rating_std'; EXECUTE sql INTO value; RETURN NEXT;

    sql := format(
        'SELECT (count(*)::double precision /
                 (count(DISTINCT %I)::double precision *
                  count(DISTINCT %I)::double precision)) * 100.0
         FROM %I',
        user_col, item_col, interactions_table);
    metric := 'density_pct'; EXECUTE sql INTO value; RETURN NEXT;

    sql := format(
        'SELECT avg(c)::double precision FROM
         (SELECT count(*) AS c FROM %I GROUP BY %I) t',
        interactions_table, user_col);
    metric := 'avg_ratings_per_user'; EXECUTE sql INTO value; RETURN NEXT;

    sql := format(
        'SELECT avg(c)::double precision FROM
         (SELECT count(*) AS c FROM %I GROUP BY %I) t',
        interactions_table, item_col);
    metric := 'avg_ratings_per_item'; EXECUTE sql INTO value; RETURN NEXT;
END;
$$;

CREATE FUNCTION recdb2_create_recommender(name text, algorithm text, config_json_text text)
RETURNS bigint LANGUAGE plpgsql AS $$
BEGIN
    RETURN recdb2_create_recommender_core(name, algorithm, config_json_text);
END;
$$;
