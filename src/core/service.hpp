#pragma once

#include "src/algorithms/algorithm.hpp"
#include <cstdint>
#include <string>

namespace recdb2::core {

class RecommenderService final {
   public:
    std::string Hello() const;

    std::int64_t CreateRecommender(const std::string& name, const std::string& algorithm,
                                   const std::string& config_json_text) const;

    std::string Train(const std::string& name) const;

    std::vector<algorithm::Prediction> Recommend(const std::string& name, std::int64_t user_id,
                                                 int top_n) const;

    std::string Retrain(const std::string& name) const;

    std::string Drop(const std::string& name) const;
};

}  // namespace recdb2::core
