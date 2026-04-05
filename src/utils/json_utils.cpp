#include "json_utils.hpp"
#include "../pg_spi/execute.hpp"
#include "src/utils/validate.hpp"

namespace recdb2::utils {

std::optional<std::string> JsonGetOptional(const std::string& json_text, const char* key) {
    std::string query = std::string("SELECT ($1::jsonb ->> '") + key + "')::text";

    auto rs = recdb2::pg_spi::Execute(query.c_str(), json_text);
    if (rs.IsEmpty() || rs[0][0].IsNull()) {
        return std::nullopt;
    }

    auto result = rs[0][0].As<std::string>();
    utils::ValidateIdentifier(result, key);
    return result;
}

std::string JsonGet(const std::string& json_text, const char* key) {
    auto result = JsonGetOptional(json_text, key);
    if (!result.has_value()) {
        ereport(ERROR, (errmsg("recdb2: missing config key '%s'", key)));
    };
    return result.value();
}

}  // namespace recdb2::utils