#pragma once

#include "config.hpp"
#include "data_loader.hpp"
#include "model.hpp"

#include <string>
#include <vector>

namespace recdb2::algorithm::fnn {

LearnedFnnState TrainFnn(const FnnConfig& cfg, const std::vector<AtomDef>& atoms,
                          const std::vector<TrainingSample>& train_samples,
                          const std::vector<TrainingSample>& test_samples,
                          const LearnedFnnState* prev_state = nullptr);

void SplitTrainTest(std::vector<TrainingSample>& all, double test_split, long seed,
                    std::vector<TrainingSample>* train_out,
                    std::vector<TrainingSample>* test_out);

}
