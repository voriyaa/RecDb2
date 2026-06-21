// Параллельное обучение FNN: data-parallel Local SGD на background-воркерах + DSM.
// Набор лежит ОДИН раз в DSM, N воркеров считают градиенты на своих шардах, после
// каждой эпохи параметры усредняются через Postgres Barrier (Zinkevich NeurIPS'10,
// Stich «Local SGD» ICLR'19; родословная — Hogwild!, Bismarck SIGMOD'12).
// Зовётся из TrainFnn при cfg.training.parallel_workers > 1.

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

}  // namespace recdb2::algorithm::fnn
