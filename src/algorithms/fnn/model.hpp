// Persistent state of a trained Fuzzy Neural Network.
// Saved to recdb2_models.learned_state as JSONB.

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
    Binary = 0,   // bool / categorical / binary threshold
    Gaussian = 1, // exp(-(x-μ)²/2σ²) с обучаемыми μ, σ на нормализованном [0,1]
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
    // Опциональный value-фильтр items: предикат "m.<col> <op> '<value>'" (op ∈ =,<,>,<=,>=).
    // Пустой op = равенство; пустой value = дефолт (col = true для boolean).
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
    int n_slots = 0;  // 0 = legacy (Bartl); >0 = NAS-fuzzy (weights = logits)
    std::vector<AtomDef> atoms;
    std::vector<double> weights;          // NAS logits, size n_rules*n_slots*n_atoms
    std::vector<double> tnorm_logits;     // size n_rules*3 — обучаемая смесь t-норм (product/softmin/mean)
    std::vector<double> gaussian_mu;      // size n_atoms — центр для membership Gaussian
    std::vector<double> gaussian_sigma;   // size n_atoms — ширина
    TrainingMetrics metrics;
    std::string trained_at;

    int Index(int rule_i, int atom_j) const { return rule_i * n_atoms + atom_j; }

    double W(int rule_i, int atom_j) const { return weights[Index(rule_i, atom_j)]; }
};

std::string SerializeFnnState(const LearnedFnnState& state);

LearnedFnnState DeserializeFnnState(const std::string& json);

const char* AtomKindName(AtomKind kind);

AtomKind ParseAtomKind(const std::string& s);

}  // namespace recdb2::algorithm::fnn
