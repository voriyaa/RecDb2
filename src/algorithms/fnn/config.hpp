#pragma once

#include "model.hpp"

#include <optional>
#include <string>
#include <vector>

namespace recdb2::algorithm::fnn {

struct InteractionsConfig {
    std::string table;
    std::string user_col;
    std::string item_col;
    std::string rating_col;
    std::string ts_col;
};

struct UsersConfig {
    std::string table;
    std::string id_col;
};

struct ItemsConfig {
    std::string table;
    std::string id_col;
};

struct TrainingConfig {
    int n_rules = 4;
    int n_slots = 4;
    double tau_start = 1.5;
    double tau_end = 0.05;
    double lambda_diversity = 0.3;
    int epochs = 50;
    double learning_rate = 0.05;
    int batch_size = 1024;
    double test_split = 0.2;
    long random_seed = 42;
    int max_train_samples = 2000000;
    double adam_beta1 = 0.9;
    double adam_beta2 = 0.999;
    double adam_epsilon = 1e-8;
    bool include_default_atoms = true;
    bool warm_start = true;
    double warm_start_lr_scale = 0.3;
    double warm_start_epoch_scale = 0.3;
    int parallel_workers = 1;
};

struct AtomConfig {
    std::string name;
    AtomKind kind;
    std::string aggregate_fn;
    std::string column;
    std::string threshold_spec;
    bool negate = false;
    std::string categorical_value;
    std::string filter_item_column;
    std::string filter_item_value;
    std::string filter_item_op;
    bool gaussian = false;
};

struct FnnConfig {
    InteractionsConfig interactions;
    std::optional<ItemsConfig> items;
    std::optional<UsersConfig> users;
    std::vector<AtomConfig> atoms;
    TrainingConfig training;
};

FnnConfig ParseFnnConfig(const std::string& config_json);

}
