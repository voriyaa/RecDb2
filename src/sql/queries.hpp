#pragma once

namespace recdb2::sql {

inline constexpr const char* kInsertModel = 
    "INSERT INTO recdb2_models(name, algorithm, config_json) "
    "VALUES ($1, $2, $3::jsonb) "
    "RETURNING id ";

} // namespace recdb2::sql