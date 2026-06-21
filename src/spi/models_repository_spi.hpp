#pragma once

#include "../storage/models_repository.hpp"

namespace recdb2::spi {

class ModelsRepositorySpi final : public recdb2::storages::ModelsRepository {
   public:
    std::int64_t InsertModel(const std::string& name, const std::string& algorithm,
                             const std::string& config_json_text) override;
};

}  // namespace recdb2::spi