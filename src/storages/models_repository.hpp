#pragma once

#include <cstdint>
#include <string>

namespace recdb2::storages {

class ModelsRepository {
   public:
    virtual ~ModelsRepository() = default;

    virtual std::int64_t InsertModel(const std::string& name, const std::string& algorithm,
                                     const std::string& config_json_text) = 0;
};

}  // namespace recdb2::storages
