#pragma once

#include <cstdint>
#include <string>

namespace recdb2::core {

class RecommenderService final {
public:
    std::string Hello() const;

    std::int64_t CreateRecommender(
        const std::string& name,
        const std::string& algorithm,
        const std::string& config_json_text
    ) const;
};

}  // namespace recdb2::core
