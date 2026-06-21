#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace recdb2::algorithm::fnn {

enum class AtomKind {
    ItemsAggregate,
    UserAggregate,
    ItemsColumn,
    UserColumn,
};

enum class MembershipKind {
    Binary = 0,
    Gaussian = 1,
};

struct AtomDef {
    std::string name;
    AtomKind kind = AtomKind::ItemsAggregate;
    std::string aggregate_fn;
    std::string column;
    double threshold = 0.0;
    double scale = 0.0;
    bool negate = false;
    std::string categorical_value;
    std::string filter_item_column;
    std::string filter_item_value;
    std::string filter_item_op;
    std::string description;
    MembershipKind membership = MembershipKind::Binary;
};

struct TrainingMetrics {
    int n_train = 0;
    int n_test = 0;
    int epochs_trained = 0;
    double final_train_loss = 0.0;
    double final_val_loss = 0.0;
    double precision_at_5 = 0.0;
    double recall_at_5 = 0.0;
    double ndcg_at_5 = 0.0;
    double map_at_5 = 0.0;
    double precision_at_10 = 0.0;
    double recall_at_10 = 0.0;
    double ndcg_at_10 = 0.0;
    double map_at_10 = 0.0;
};

struct LearnedFnnState {
    int n_atoms = 0;
    int n_rules = 0;
    int n_slots = 0;
    std::vector<AtomDef> atoms;
    std::vector<double> weights;
    std::vector<double> tnorm_logits;
    std::vector<double> gaussian_mu;
    std::vector<double> gaussian_sigma;
    TrainingMetrics metrics;
    std::string trained_at;

    int Index(int rule_i, int atom_j) const { return rule_i * n_atoms + atom_j; }

    double W(int rule_i, int atom_j) const { return weights[Index(rule_i, atom_j)]; }
};

std::string SerializeFnnState(const LearnedFnnState& state);

LearnedFnnState DeserializeFnnState(const std::string& json);

const char* AtomKindName(AtomKind kind);

AtomKind ParseAtomKind(const std::string& s);

}
