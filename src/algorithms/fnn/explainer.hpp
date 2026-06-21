#pragma once

#include "../algorithm.hpp"
#include "data_loader.hpp"
#include "model.hpp"

#include <vector>

namespace recdb2::algorithm::fnn {

std::vector<ExplanationItem> ExplainPrediction(const LearnedFnnState& state,
                                                const InferenceItem& sample,
                                                std::int64_t user_id);

std::vector<ExplanationItem> IntrospectModel(const LearnedFnnState& state);

}
