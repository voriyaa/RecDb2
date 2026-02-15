CREATE FUNCTION recdb2_create_recommender(name text, algorithm text, config_json_text text)
RETURNS bigint
AS 'MODULE_PATHNAME', 'recdb2_create_recommender'
LANGUAGE C STRICT;
