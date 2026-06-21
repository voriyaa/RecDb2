#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace recdb2::algorithm {

struct Prediction {
    std::int64_t item_id;
    double score;
};

struct ExplanationItem {
    std::string kind;
    std::string label;
    double contribution;
    std::string details_json;
};

class Algorithm {
   public:
    virtual std::vector<std::string> RequiredConfigKeys() const = 0;

    virtual void Train(std::int64_t model_id, const std::string& config_json) = 0;

    virtual std::vector<Prediction> Recommend(std::int64_t model_id, std::int64_t user_id,
                                              int top_n, const std::string& config_json) = 0;

    virtual std::vector<ExplanationItem> Explain(std::int64_t model_id, std::int64_t user_id,
                                                 std::int64_t item_id,
                                                 const std::string& config_json) = 0;

    virtual std::vector<ExplanationItem> Introspect(std::int64_t model_id,
                                                    const std::string& config_json,
                                                    const std::string& learned_state_json) {
        (void)model_id;
        (void)config_json;
        (void)learned_state_json;
        return {};
    }

    virtual double Score(std::int64_t model_id, std::int64_t user_id, std::int64_t item_id,
                         const std::string& config_json) = 0;

    virtual ~Algorithm() = default;
};

}  // namespace recdb2::algorithm
