#include "service.hpp"
#include "../pg_spi/models_repository_spi.hpp"

namespace recdb2::core {

std::string RecommenderService::Hello() const {
    return "recdb2: hello";
}

std::int64_t RecommenderService::CreateRecommender(
    const std::string& name,
    const std::string& algorithm,
    const std::string& config_json_text
) const {
    recdb2::pg_spi::ModelsRepositorySpi repo;
    return repo.InsertModel(name, algorithm, config_json_text);
}

}  // namespace recdb2::core
