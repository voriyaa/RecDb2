#include "execute.hpp"
#include "models_repository_spi.hpp"
#include "../core/queries.hpp"

namespace recdb2::spi {

std::int64_t ModelsRepositorySpi::InsertModel(const std::string& name, const std::string& algorithm,
                                              const std::string& config_json_text) {
    auto result = Execute(recdb2::sql::kInsertModel, name, algorithm, config_json_text);
    return result.SingleRow()[0].As<std::int64_t>();
}

}  // namespace recdb2::spi