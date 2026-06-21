#pragma once

#include "data_loader.hpp"
#include "metrics.hpp"
#include "model.hpp"
#include "nn.hpp"

#include <vector>

namespace recdb2::algorithm::fnn {

// Примитивы NAS-fuzzy обучения, общие для серийного (trainer.cpp) и
// параллельного (parallel_trainer.cpp) путей: расписания lr/τ, конвертер
// membership и оценка на отложенной выборке. Вынесены сюда, чтобы адаптер под
// движок parallel_sgd оставался тонким и не дублировал серийный тренер.

std::vector<AtomMembership> MakeMembership(const std::vector<AtomDef>& atoms);

double LrSchedule(int epoch, int total_epochs);

double TauSchedule(int epoch, int total_epochs, double tau_start, double tau_end);

double EvalMseNas(const std::vector<TrainingSample>& samples, const std::vector<double>& logits,
                  const std::vector<double>& mu, const std::vector<double>& sigma,
                  const std::vector<AtomMembership>& membership, int n_rules, int n_slots,
                  int n_atoms, double tau);

std::vector<UserItemScore> ScoreSamplesNas(const std::vector<TrainingSample>& samples,
                                           const std::vector<double>& logits,
                                           const std::vector<double>& mu,
                                           const std::vector<double>& sigma,
                                           const std::vector<AtomMembership>& membership,
                                           int n_rules, int n_slots, int n_atoms, double tau);

}  // namespace recdb2::algorithm::fnn
