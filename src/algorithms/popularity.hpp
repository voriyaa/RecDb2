#pragma once

#include "algorithm.hpp"

namespace recdb2::algorithm {

class PopularityAlgorithm final : public Algorithm {
   public:
    std::vector<std::string> RequiredConfigKeys() const override;

    void Train(std::int64_t model_id, const std::string& config_json) override;

    std::vector<Prediction> Recommend(std::int64_t model_id, std::int64_t user_id, int top_n,
                                      const std::string& config_json) override;
};

}  // namespace recdb2::algorithm