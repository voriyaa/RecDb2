#pragma once

namespace recdb2::sql {

// ---- models ----

inline constexpr const char* kInsertModel =
    "INSERT INTO recdb2_models(name, algorithm, config_json) "
    "VALUES ($1, $2, $3::jsonb) "
    "RETURNING id ";

inline constexpr const char* kSelectModelByName =
    "SELECT id, name, algorithm, state, config_json::text "
    "FROM recdb2_models "
    "WHERE name = $1 ";

inline constexpr const char* kUpdateModelState =
    "UPDATE recdb2_models "
    "SET state = $2, updated_at = now(), last_error = $3 "
    "WHERE id = $1 ";

inline constexpr const char* kDeleteModel = "DELETE FROM recdb2_models WHERE name = $1 ";

// ---- predictions ---

inline constexpr const char* kDeletePredictions =
    "DELETE FROM recdb2_predictions WHERE model_id = $1 ";

inline constexpr const char* kInsertPrediction =
    "INSERT INTO recdb2_predictions(model_id, item_id, score) "
    "VALUES ($1, $2, $3)"
    "ON CONFLICT (model_id, item_id) DO UPDATE score = EXCLUDED.score ";

inline constexpr const char* kSelectTopPredictions =
    "SELECT p.item_id, p.score "
    "FROM recdb2_predictions p "
    "WHERE p.model_id = $1 "
    "ORDER BY p.score DESC "
    "LIMIT $2 ";

}  // namespace recdb2::sql