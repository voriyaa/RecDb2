#pragma once

#include "config.hpp"
#include "model.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace recdb2::algorithm::fnn {

struct TrainingSample {
    std::int64_t user_id = 0;
    std::int64_t item_id = 0;
    double target = 0.0;
    std::vector<double> atoms;
};

struct InferenceItem {
    std::int64_t item_id = 0;
    std::vector<double> atoms;
};

void CreateStatsViews(const FnnConfig& cfg, const std::vector<AtomDef>& atoms,
                       const std::string& item_stats_view, const std::string& user_stats_view);

void DropStatsViews(const std::string& item_stats_view, const std::string& user_stats_view);

std::vector<TrainingSample> LoadTrainingSet(const FnnConfig& cfg,
                                             const std::vector<AtomDef>& atoms,
                                             const std::string& item_stats_view,
                                             const std::string& user_stats_view);

void LoadDistinctUserItemIds(const FnnConfig& cfg, std::vector<std::int64_t>* user_ids,
                              std::vector<std::int64_t>* item_ids);

std::vector<InferenceItem> LoadInferenceItems(const FnnConfig& cfg,
                                               const std::vector<AtomDef>& atoms,
                                               const std::string& item_stats_view,
                                               const std::string& user_stats_view,
                                               std::int64_t user_id, bool exclude_rated);

std::optional<InferenceItem> LoadSingleInferenceItem(const FnnConfig& cfg,
                                                     const std::vector<AtomDef>& atoms,
                                                     const std::string& item_stats_view,
                                                     const std::string& user_stats_view,
                                                     std::int64_t user_id,
                                                     std::int64_t item_id);

}  // namespace recdb2::algorithm::fnn
