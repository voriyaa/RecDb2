// FnnAlgorithm — реализация интерфейса Algorithm для нечёткой нейросети
// (Bartl et al. 2025). Здесь только оркестровка SPI-вызовов и
// process-локальный кеш состояния. Вся математика — в nn.cpp / trainer.cpp.

#include "fnn.hpp"

#include "atom_builder.hpp"
#include "config.hpp"
#include "data_loader.hpp"
#include "explainer.hpp"
#include "model.hpp"
#include "nn.hpp"
#include "trainer.hpp"

#include "src/core/queries.hpp"
#include "src/spi/execute.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>

namespace recdb2::algorithm::fnn {

namespace {

std::string ItemStatsViewName(std::int64_t model_id) {
    return "recdb2_fnn_" + std::to_string(model_id) + "_item_atoms";
}

std::string UserStatsViewName(std::int64_t model_id) {
    return "recdb2_fnn_" + std::to_string(model_id) + "_user_atoms";
}

// Per-backend cache. JSONB-десериализация модели стоит ~200 мс на ML-1M;
// без кеша recdb2_score становится непригодным для batch-запросов.
struct CachedState {
    std::shared_ptr<LearnedFnnState> state;
    std::string updated_at;
};

std::unordered_map<std::int64_t, CachedState>& StateCache() {
    static std::unordered_map<std::int64_t, CachedState> cache;
    return cache;
}

void InvalidateCache(std::int64_t model_id) { StateCache().erase(model_id); }

std::shared_ptr<LearnedFnnState> LoadOrFail(std::int64_t model_id) {
    static constexpr const char* kVersionQuery =
        "SELECT updated_at::text FROM recdb2_models WHERE id = $1 ";
    auto vr = spi::Execute(kVersionQuery, model_id);
    if (vr.IsEmpty()) {
        ereport(ERROR, (errmsg("recdb2/fnn: model id=%lld not found",
                               static_cast<long long>(model_id))));
    }
    const std::string updated_at = vr.SingleRow()[0].As<std::string>();

    auto& cache = StateCache();
    auto it = cache.find(model_id);
    if (it != cache.end() && it->second.updated_at == updated_at) {
        return it->second.state;
    }

    static constexpr const char* kJsonQuery =
        "SELECT COALESCE(learned_state::text, '{}') FROM recdb2_models WHERE id = $1 ";
    auto rs = spi::Execute(kJsonQuery, model_id);
    const std::string json = rs.SingleRow()[0].As<std::string>();
    auto state = std::make_shared<LearnedFnnState>(DeserializeFnnState(json));
    if (state->n_atoms == 0 || state->weights.empty()) {
        ereport(ERROR, (errmsg("recdb2/fnn: model id=%lld has no trained state",
                               static_cast<long long>(model_id))));
    }
    cache[model_id] = {state, updated_at};
    return state;
}

}  // namespace

std::vector<std::string> FnnAlgorithm::RequiredConfigKeys() const { return {}; }

void FnnAlgorithm::Train(std::int64_t model_id, const std::string& config_json) {
    const auto cfg = ParseFnnConfig(config_json);

    ereport(NOTICE, (errmsg("recdb2/fnn: building atoms")));
    const auto atoms = BuildResolvedAtoms(cfg);
    if (atoms.empty()) {
        ereport(ERROR, (errmsg("recdb2/fnn: no atoms resolved")));
    }
    ereport(NOTICE,
            (errmsg("recdb2/fnn: %d atoms resolved", static_cast<int>(atoms.size()))));

    const auto item_view = ItemStatsViewName(model_id);
    const auto user_view = UserStatsViewName(model_id);

    ereport(NOTICE, (errmsg("recdb2/fnn: building stats materialized views")));
    CreateStatsViews(cfg, atoms, item_view, user_view);

    ereport(NOTICE, (errmsg("recdb2/fnn: loading training set (max %d)",
                             cfg.training.max_train_samples)));
    auto samples = LoadTrainingSet(cfg, atoms, item_view, user_view);
    if (samples.empty()) {
        ereport(ERROR, (errmsg("recdb2/fnn: empty training set after load")));
    }
    ereport(NOTICE,
            (errmsg("recdb2/fnn: loaded %d samples", static_cast<int>(samples.size()))));

    std::vector<TrainingSample> train_set;
    std::vector<TrainingSample> test_set;
    SplitTrainTest(samples, cfg.training.test_split, cfg.training.random_seed, &train_set,
                    &test_set);
    ereport(NOTICE, (errmsg("recdb2/fnn: split %d train / %d test",
                             static_cast<int>(train_set.size()),
                             static_cast<int>(test_set.size()))));

    LearnedFnnState prev_state;
    bool have_prev_state = false;
    if (cfg.training.warm_start) {
        static constexpr const char* kQuery =
            "SELECT COALESCE(learned_state::text, '{}') FROM recdb2_models WHERE id = $1 ";
        auto rs = spi::Execute(kQuery, model_id);
        if (!rs.IsEmpty() && !rs[0][0].IsNull()) {
            const std::string json = rs.SingleRow()[0].As<std::string>();
            if (!json.empty() && json != "{}") {
                prev_state = DeserializeFnnState(json);
                have_prev_state = !prev_state.weights.empty();
            }
        }
    }

    auto state = TrainFnn(cfg, atoms, train_set, test_set,
                            have_prev_state ? &prev_state : nullptr);

    static constexpr const char* kTimestampQuery = "SELECT now()::text ";
    auto ts_rs = spi::Execute(kTimestampQuery);
    if (!ts_rs.IsEmpty() && !ts_rs[0][0].IsNull()) {
        state.trained_at = ts_rs[0][0].As<std::string>();
    }

    const auto state_json = SerializeFnnState(state);
    spi::Execute(sql::kUpdateLearnedState, model_id, state_json);
    InvalidateCache(model_id);

    ereport(NOTICE, (errmsg("recdb2/fnn: model id=%lld training complete",
                             static_cast<long long>(model_id))));
}

namespace {
std::vector<AtomMembership> MembershipFor(const LearnedFnnState& state) {
    std::vector<AtomMembership> out(state.atoms.size());
    for (std::size_t i = 0; i < state.atoms.size(); ++i) {
        out[i].kind = (state.atoms[i].membership == MembershipKind::Gaussian) ? 1 : 0;
    }
    return out;
}

double ScoreOne(const LearnedFnnState& state, const std::vector<double>& atoms) {
    NasForwardCache c;
    const auto membership = MembershipFor(state);
    // Inference: tau близок к 0 → ~hard discrete rules.
    return ForwardPassNas(state.weights, state.gaussian_mu, state.gaussian_sigma, membership,
                          state.n_rules, state.n_slots, state.n_atoms, atoms, 0.1, &c);
}
}  // namespace

std::vector<Prediction> FnnAlgorithm::Recommend(std::int64_t model_id, std::int64_t user_id,
                                                  int top_n, const std::string& config_json) {
    const auto cfg = ParseFnnConfig(config_json);
    const auto state_ptr = LoadOrFail(model_id);
    const auto& state = *state_ptr;

    auto inference =
        LoadInferenceItems(cfg, state.atoms, ItemStatsViewName(model_id),
                            UserStatsViewName(model_id), user_id, /*exclude_rated=*/true);

    std::vector<Prediction> scored;
    scored.reserve(inference.size());
    for (const auto& it : inference) {
        scored.push_back({it.item_id, ScoreOne(state, it.atoms)});
    }

    std::sort(scored.begin(), scored.end(),
              [](const Prediction& a, const Prediction& b) { return a.score > b.score; });
    if (static_cast<int>(scored.size()) > top_n) {
        scored.resize(static_cast<std::size_t>(top_n));
    }
    return scored;
}

std::vector<ExplanationItem> FnnAlgorithm::Explain(std::int64_t model_id, std::int64_t user_id,
                                                     std::int64_t item_id,
                                                     const std::string& config_json) {
    const auto cfg = ParseFnnConfig(config_json);
    const auto state_ptr = LoadOrFail(model_id);
    const auto& state = *state_ptr;

    auto sample = LoadSingleInferenceItem(cfg, state.atoms, ItemStatsViewName(model_id),
                                            UserStatsViewName(model_id), user_id, item_id);
    if (!sample.has_value()) {
        ereport(ERROR, (errmsg("recdb2/fnn: cannot evaluate (user_id=%lld, item_id=%lld)",
                               static_cast<long long>(user_id),
                               static_cast<long long>(item_id))));
    }
    return ExplainPrediction(state, *sample, user_id);
}

double FnnAlgorithm::Score(std::int64_t model_id, std::int64_t user_id, std::int64_t item_id,
                            const std::string& config_json) {
    const auto cfg = ParseFnnConfig(config_json);
    const auto state_ptr = LoadOrFail(model_id);
    const auto& state = *state_ptr;

    auto sample = LoadSingleInferenceItem(cfg, state.atoms, ItemStatsViewName(model_id),
                                            UserStatsViewName(model_id), user_id, item_id);
    if (!sample.has_value()) return 0.0;
    return ScoreOne(state, sample->atoms);
}

std::vector<ExplanationItem> FnnAlgorithm::Introspect(std::int64_t /*model_id*/,
                                                       const std::string& /*config_json*/,
                                                       const std::string& learned_state_json) {
    const auto state = DeserializeFnnState(learned_state_json);
    return IntrospectModel(state);
}

}  // namespace recdb2::algorithm::fnn
