#include "parallel_trainer.hpp"

#include "../parallel/parallel_sgd.hpp"
#include "metrics.hpp"
#include "nn.hpp"
#include "training_support.hpp"

extern "C" {
#include "postgres.h"

#include "utils/elog.h"
}

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <memory>
#include <random>
#include <vector>

namespace recdb2::algorithm::fnn {

namespace {

constexpr int kAlgoFnn = 1;

struct FnnHyper {
    std::int32_t n_atoms;
    std::int32_t n_rules;
    std::int32_t n_slots;
    std::int32_t n_gauss;
    std::int32_t n_logits;
    double learning_rate;
    double adam_beta1;
    double adam_beta2;
    double adam_epsilon;
    double effective_tau_start;
    double tau_end;
    double lambda_diversity;
    double lr_scale_extra;
};

class FnnSgdModel : public recdb2::parallel::ParallelSgdModel {
   public:
    FnnSgdModel(const FnnHyper& hp, std::vector<AtomMembership> membership)
        : n_atoms_(hp.n_atoms),
          n_rules_(hp.n_rules),
          n_slots_(hp.n_slots),
          n_gauss_(hp.n_gauss),
          n_logits_(hp.n_logits),
          eff_tau_start_(hp.effective_tau_start),
          tau_end_(hp.tau_end),
          lambda_div_(hp.lambda_diversity),
          lr_scale_extra_(hp.lr_scale_extra),
          membership_(std::move(membership)),
          logits_(static_cast<std::size_t>(hp.n_logits), 0.0),
          mu_(static_cast<std::size_t>(hp.n_gauss), 0.0),
          sigma_(static_cast<std::size_t>(hp.n_gauss), 0.0),
          opt_logits_(static_cast<std::size_t>(hp.n_logits), hp.learning_rate, hp.adam_beta1,
                      hp.adam_beta2, hp.adam_epsilon),
          opt_gauss_(static_cast<std::size_t>(2 * hp.n_gauss), hp.learning_rate * 0.5,
                     hp.adam_beta1, hp.adam_beta2, hp.adam_epsilon),
          batch_logit_grad_(static_cast<std::size_t>(hp.n_logits), 0.0),
          batch_gauss_grad_(static_cast<std::size_t>(2 * hp.n_gauss), 0.0),
          single_logit_(static_cast<std::size_t>(hp.n_logits), 0.0),
          single_mu_(static_cast<std::size_t>(hp.n_gauss), 0.0),
          single_sigma_(static_cast<std::size_t>(hp.n_gauss), 0.0),
          gauss_concat_(static_cast<std::size_t>(2 * hp.n_gauss), 0.0),
          atoms_scratch_(static_cast<std::size_t>(hp.n_atoms), 0.0) {}

    int ParamVectorSize() const override { return n_logits_ + 2 * n_gauss_; }

    void ExportParams(double* out) const override {
        std::copy(logits_.begin(), logits_.end(), out);
        std::copy(mu_.begin(), mu_.end(), out + n_logits_);
        std::copy(sigma_.begin(), sigma_.end(), out + n_logits_ + n_gauss_);
    }

    void ImportParams(const double* in) override {
        std::copy(in, in + n_logits_, logits_.begin());
        std::copy(in + n_logits_, in + n_logits_ + n_gauss_, mu_.begin());
        std::copy(in + n_logits_ + n_gauss_, in + n_logits_ + 2 * n_gauss_, sigma_.begin());
    }

    void TrainShardEpoch(int epoch, int total_epochs, const float* features, const double* targets,
                         int* shard_idx, int shard_len, int n_features, int batch_size,
                         std::mt19937_64& rng) override {
        std::shuffle(shard_idx, shard_idx + shard_len, rng);
        const double lr_scale = LrSchedule(epoch, total_epochs) * lr_scale_extra_;
        const double tau = TauSchedule(epoch, total_epochs, eff_tau_start_, tau_end_);

        for (int start = 0; start < shard_len; start += batch_size) {
            const int end = std::min(shard_len, start + batch_size);
            const int actual = end - start;
            if (actual == 0) continue;

            std::fill(batch_logit_grad_.begin(), batch_logit_grad_.end(), 0.0);
            std::fill(batch_gauss_grad_.begin(), batch_gauss_grad_.end(), 0.0);
            for (int b = start; b < end; ++b) {
                const int si = shard_idx[b];
                const float* ap = features + static_cast<std::size_t>(si) * n_features;
                for (int j = 0; j < n_atoms_; ++j) atoms_scratch_[j] = static_cast<double>(ap[j]);

                ForwardPassNas(logits_, mu_, sigma_, membership_, n_rules_, n_slots_, n_atoms_,
                                atoms_scratch_, tau, &cache_);
                BackwardPassNas(cache_, atoms_scratch_, mu_, sigma_, membership_, targets[si],
                                 n_rules_, n_slots_, n_atoms_, tau, &single_logit_, &single_mu_,
                                 &single_sigma_);
                for (int k = 0; k < n_logits_; ++k) batch_logit_grad_[k] += single_logit_[k];
                for (int j = 0; j < n_gauss_; ++j) {
                    batch_gauss_grad_[j] += single_mu_[j];
                    batch_gauss_grad_[n_gauss_ + j] += single_sigma_[j];
                }
            }
            const double inv = 1.0 / static_cast<double>(actual);
            for (auto& g : batch_logit_grad_) g *= inv;
            for (auto& g : batch_gauss_grad_) g *= inv;

            if (lambda_div_ > 0.0) {
                AddDiversityGradient(cache_, n_rules_, n_slots_, n_atoms_, lambda_div_, tau,
                                      &batch_logit_grad_);
            }
            opt_logits_.Step(logits_, batch_logit_grad_, lr_scale);

            for (int j = 0; j < n_gauss_; ++j) {
                gauss_concat_[j] = mu_[j];
                gauss_concat_[n_gauss_ + j] = sigma_[j];
            }
            opt_gauss_.Step(gauss_concat_, batch_gauss_grad_, lr_scale);
            for (int j = 0; j < n_gauss_; ++j) {
                mu_[j] = gauss_concat_[j];
                sigma_[j] = std::max(0.05, std::min(1.0, gauss_concat_[n_gauss_ + j]));
            }
        }
    }

   private:
    int n_atoms_;
    int n_rules_;
    int n_slots_;
    int n_gauss_;
    int n_logits_;
    double eff_tau_start_;
    double tau_end_;
    double lambda_div_;
    double lr_scale_extra_;
    std::vector<AtomMembership> membership_;
    std::vector<double> logits_;
    std::vector<double> mu_;
    std::vector<double> sigma_;
    AdamOptimizer opt_logits_;
    AdamOptimizer opt_gauss_;
    NasForwardCache cache_;
    std::vector<double> batch_logit_grad_;
    std::vector<double> batch_gauss_grad_;
    std::vector<double> single_logit_;
    std::vector<double> single_mu_;
    std::vector<double> single_sigma_;
    std::vector<double> gauss_concat_;
    std::vector<double> atoms_scratch_;
};

std::unique_ptr<recdb2::parallel::ParallelSgdModel> MakeFnnSgdModel(
    const recdb2::parallel::ParallelSgdDims& dims, const void* hyper, std::size_t hyper_len) {
    (void)dims;
    (void)hyper_len;
    const auto* hp = static_cast<const FnnHyper*>(hyper);
    const auto* memb_bytes = static_cast<const std::uint8_t*>(hyper) + sizeof(FnnHyper);
    std::vector<AtomMembership> membership(static_cast<std::size_t>(hp->n_atoms));
    for (int j = 0; j < hp->n_atoms; ++j) membership[j].kind = memb_bytes[j];
    return std::make_unique<FnnSgdModel>(*hp, std::move(membership));
}

struct FnnRegistrar {
    FnnRegistrar() { recdb2::parallel::RegisterParallelSgdModel(kAlgoFnn, &MakeFnnSgdModel); }
};
const FnnRegistrar g_fnn_registrar;

}

LearnedFnnState TrainFnnParallel(const FnnConfig& cfg, const std::vector<AtomDef>& atoms,
                                 const std::vector<TrainingSample>& train_samples,
                                 const std::vector<TrainingSample>& test_samples,
                                 const LearnedFnnState* prev_state) {
    const int n_atoms = static_cast<int>(atoms.size());
    const int n_rules = cfg.training.n_rules;
    const int n_slots = std::min(cfg.training.n_slots, n_atoms);
    const int n_logits = n_rules * n_slots * n_atoms;
    const int n_gauss = n_atoms;
    const int param_block = n_logits + 2 * n_gauss;
    const int n_samples = static_cast<int>(train_samples.size());

    const auto membership = MakeMembership(atoms);

    const bool warm_start = prev_state != nullptr && cfg.training.warm_start &&
                              prev_state->n_slots == n_slots &&
                              static_cast<int>(prev_state->weights.size()) == n_logits &&
                              static_cast<int>(prev_state->gaussian_mu.size()) == n_gauss;

    std::vector<double> init_logits;
    std::vector<double> init_mu(n_gauss, 0.5);
    std::vector<double> init_sigma(n_gauss, 0.3);
    if (warm_start) {
        init_logits = prev_state->weights;
        init_mu = prev_state->gaussian_mu;
        init_sigma = prev_state->gaussian_sigma;
    } else {
        InitializeLogitsNas(init_logits, n_rules, n_slots, n_atoms, cfg.training.random_seed);
    }

    const int epochs =
        warm_start ? std::max(1, static_cast<int>(std::round(
                                     cfg.training.epochs * cfg.training.warm_start_epoch_scale)))
                   : std::max(1, cfg.training.epochs);
    const double lr_scale_extra = warm_start ? cfg.training.warm_start_lr_scale : 1.0;
    const double effective_tau_start =
        warm_start ? std::max(cfg.training.tau_end * 2.0, 0.2) : cfg.training.tau_start;

    std::vector<float> features(static_cast<std::size_t>(n_samples) * n_atoms);
    for (int i = 0; i < n_samples; ++i) {
        const auto& a = train_samples[i].atoms;
        float* dst = features.data() + static_cast<std::size_t>(i) * n_atoms;
        for (int j = 0; j < n_atoms; ++j) dst[j] = static_cast<float>(a[j]);
    }
    std::vector<double> targets(n_samples);
    for (int i = 0; i < n_samples; ++i) targets[i] = train_samples[i].target;

    std::vector<double> init_params(param_block);
    std::memcpy(init_params.data(), init_logits.data(), sizeof(double) * n_logits);
    std::memcpy(init_params.data() + n_logits, init_mu.data(), sizeof(double) * n_gauss);
    std::memcpy(init_params.data() + n_logits + n_gauss, init_sigma.data(),
                sizeof(double) * n_gauss);

    FnnHyper hp;
    hp.n_atoms = n_atoms;
    hp.n_rules = n_rules;
    hp.n_slots = n_slots;
    hp.n_gauss = n_gauss;
    hp.n_logits = n_logits;
    hp.learning_rate = cfg.training.learning_rate;
    hp.adam_beta1 = cfg.training.adam_beta1;
    hp.adam_beta2 = cfg.training.adam_beta2;
    hp.adam_epsilon = cfg.training.adam_epsilon;
    hp.effective_tau_start = effective_tau_start;
    hp.tau_end = cfg.training.tau_end;
    hp.lambda_diversity = cfg.training.lambda_diversity;
    hp.lr_scale_extra = lr_scale_extra;

    std::vector<unsigned char> hyper(sizeof(FnnHyper) + static_cast<std::size_t>(n_atoms));
    std::memcpy(hyper.data(), &hp, sizeof(FnnHyper));
    for (int j = 0; j < n_atoms; ++j) {
        hyper[sizeof(FnnHyper) + static_cast<std::size_t>(j)] = membership[j].kind;
    }

    recdb2::parallel::ParallelSgdSpec spec;
    spec.algo_id = kAlgoFnn;
    spec.n_samples = n_samples;
    spec.n_features = n_atoms;
    spec.epochs = epochs;
    spec.batch_size = std::max(1, cfg.training.batch_size);
    spec.random_seed = cfg.training.random_seed;
    spec.features = features.data();
    spec.targets = targets.data();
    spec.init_params = std::move(init_params);
    spec.hyper = std::move(hyper);
    spec.requested_workers = cfg.training.parallel_workers;

    const std::vector<double> final_params = recdb2::parallel::RunDataParallelSgd(spec);

    std::vector<double> final_logits(final_params.begin(), final_params.begin() + n_logits);
    std::vector<double> final_mu(final_params.begin() + n_logits,
                                 final_params.begin() + n_logits + n_gauss);
    std::vector<double> final_sigma(final_params.begin() + n_logits + n_gauss,
                                    final_params.begin() + param_block);

    LearnedFnnState state;
    state.n_atoms = n_atoms;
    state.n_rules = n_rules;
    state.n_slots = n_slots;
    state.atoms = atoms;
    state.weights = final_logits;
    state.gaussian_mu = final_mu;
    state.gaussian_sigma = final_sigma;
    state.metrics.n_train = n_samples;
    state.metrics.n_test = static_cast<int>(test_samples.size());
    state.metrics.epochs_trained = epochs;
    state.metrics.final_train_loss = EvalMseNas(train_samples, final_logits, final_mu, final_sigma,
                                                membership, n_rules, n_slots, n_atoms,
                                                cfg.training.tau_end);

    if (!test_samples.empty()) {
        state.metrics.final_val_loss = EvalMseNas(test_samples, final_logits, final_mu, final_sigma,
                                                  membership, n_rules, n_slots, n_atoms,
                                                  cfg.training.tau_end);
        auto scored = ScoreSamplesNas(test_samples, final_logits, final_mu, final_sigma, membership,
                                      n_rules, n_slots, n_atoms, cfg.training.tau_end);
        FillEvaluationMetrics(scored, &state.metrics);
        ereport(NOTICE,
                (errmsg("recdb2/fnn-parallel: val_loss=%.6f p@5=%.4f r@5=%.4f ndcg@5=%.4f "
                        "ndcg@10=%.4f",
                        state.metrics.final_val_loss, state.metrics.precision_at_5,
                        state.metrics.recall_at_5, state.metrics.ndcg_at_5,
                        state.metrics.ndcg_at_10)));
    }
    return state;
}

}
