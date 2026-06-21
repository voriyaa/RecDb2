#pragma once

#include "config.hpp"
#include "data_loader.hpp"
#include "model.hpp"

#include <vector>

namespace recdb2::algorithm::fnn {

LearnedFnnState TrainFnnParallel(const FnnConfig& cfg, const std::vector<AtomDef>& atoms,
                                 const std::vector<TrainingSample>& train_samples,
                                 const std::vector<TrainingSample>& test_samples,
                                 const LearnedFnnState* prev_state);

}
