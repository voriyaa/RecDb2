#pragma once

#include "model.hpp"

#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace recdb2::algorithm::fnn {

struct UserItemScore {
    std::int64_t user_id;
    std::int64_t item_id;
    double score;
    double target;
};

struct TopKMetrics {
    double precision = 0.0;
    double recall = 0.0;
    double ndcg = 0.0;
    double map = 0.0;
};

TopKMetrics ComputeTopKMetrics(const std::vector<UserItemScore>& predictions, int k);

void FillEvaluationMetrics(const std::vector<UserItemScore>& test_predictions,
                            TrainingMetrics* out);

}
