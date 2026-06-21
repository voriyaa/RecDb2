#pragma once

#include "../algorithm.hpp"

namespace recdb2::algorithm::fnn {

class FnnAlgorithm final : public Algorithm {
   public:
    std::vector<std::string> RequiredConfigKeys() const override;

    void Train(std::int64_t model_id, const std::string& config_json) override;

    std::vector<Prediction> Recommend(std::int64_t model_id, std::int64_t user_id, int top_n,
                                      const std::string& config_json) override;

    std::vector<ExplanationItem> Explain(std::int64_t model_id, std::int64_t user_id,
                                         std::int64_t item_id,
                                         const std::string& config_json) override;

    std::vector<ExplanationItem> Introspect(std::int64_t model_id, const std::string& config_json,
                                             const std::string& learned_state_json) override;

    double Score(std::int64_t model_id, std::int64_t user_id, std::int64_t item_id,
                 const std::string& config_json) override;
};

}
