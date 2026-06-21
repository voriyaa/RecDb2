#include "trainer.hpp"

#include "metrics.hpp"
#include "nn.hpp"
#include "parallel_trainer.hpp"
#include "training_support.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <numeric>
#include <random>

namespace recdb2::algorithm::fnn {

namespace {

LearnedFnnState TrainFnnNas(const FnnConfig& cfg, const std::vector<AtomDef>& atoms,
                             const std::vector<TrainingSample>& train_samples,
                             const std::vector<TrainingSample>& test_samples,
                             const LearnedFnnState* prev_state) {
    const int n_atoms = static_cast<int>(atoms.size());
    const int n_rules = cfg.training.n_rules;
    const int n_slots = std::min(cfg.training.n_slots, n_atoms);  // не больше чем атомов
    const std::size_t n_params =
        static_cast<std::size_t>(n_rules) * n_slots * n_atoms;

    const auto membership = MakeMembership(atoms);
    const std::size_t n_gauss = static_cast<std::size_t>(n_atoms);

    const bool warm_start = prev_state != nullptr && cfg.training.warm_start &&
                              prev_state->n_slots == n_slots &&
                              prev_state->weights.size() == n_params &&
                              prev_state->gaussian_mu.size() == n_gauss;

    std::vector<double> logits;
    std::vector<double> gaussian_mu(n_gauss, 0.5);
    std::vector<double> gaussian_sigma(n_gauss, 0.3);
    if (warm_start) {
        logits = prev_state->weights;
        gaussian_mu = prev_state->gaussian_mu;
        gaussian_sigma = prev_state->gaussian_sigma;
        ereport(NOTICE,
                (errmsg("recdb2/fnn-nas: warm-start (logits + gauss из prev state)")));
    } else {
        InitializeLogitsNas(logits, n_rules, n_slots, n_atoms, cfg.training.random_seed);
    }

    AdamOptimizer opt_logits(logits.size(), cfg.training.learning_rate, cfg.training.adam_beta1,
                              cfg.training.adam_beta2, cfg.training.adam_epsilon);
    AdamOptimizer opt_gauss(n_gauss * 2, cfg.training.learning_rate * 0.5,
                              cfg.training.adam_beta1, cfg.training.adam_beta2,
                              cfg.training.adam_epsilon);

    std::vector<std::size_t> indices(train_samples.size());
    std::iota(indices.begin(), indices.end(), 0);
    std::mt19937_64 rng(static_cast<std::uint64_t>(cfg.training.random_seed) ^
                         0x9e3779b97f4a7c15ULL);

    std::vector<double> batch_logit_grad(logits.size(), 0.0);
    std::vector<double> batch_gauss_grad(n_gauss * 2, 0.0);

    std::vector<double> single_logit(logits.size(), 0.0);
    std::vector<double> single_mu(n_gauss, 0.0);
    std::vector<double> single_sigma(n_gauss, 0.0);
    NasForwardCache cache;

    const int batch_size = std::max(1, cfg.training.batch_size);
    const int epochs =
        warm_start
            ? std::max(1, static_cast<int>(std::round(
                                cfg.training.epochs * cfg.training.warm_start_epoch_scale)))
            : std::max(1, cfg.training.epochs);
    const double lr_scale_extra = warm_start ? cfg.training.warm_start_lr_scale : 1.0;

    double last_train_loss = 0.0;
    int epochs_done = 0;
    double best_val_loss = std::numeric_limits<double>::infinity();
    std::vector<double> best_logits = logits;
    std::vector<double> best_mu = gaussian_mu;
    std::vector<double> best_sigma = gaussian_sigma;
    // Warm-start: НЕ разрушаем уже дискретные правила высокой tau. Стартуем низко
    // (чуть выше tau_end) и плавно полируем. Cold-start: полный tau_start → tau_end.
    const double effective_tau_start = warm_start
        ? std::max(cfg.training.tau_end * 2.0, 0.2)
        : cfg.training.tau_start;
    double cur_tau = effective_tau_start;
    if (warm_start) {
        ereport(NOTICE, (errmsg("recdb2/fnn-nas: warm-start tau %.3f→%.3f (без размывания)",
                                  effective_tau_start, cfg.training.tau_end)));
    }

    double sgd_seconds = 0.0;  // только SGD-фаза (без поэпошной валидации) — для сравнения с parallel
    for (int epoch = 0; epoch < epochs; ++epoch) {
        const auto sgd_t0 = std::chrono::steady_clock::now();
        std::shuffle(indices.begin(), indices.end(), rng);
        const double lr_scale = LrSchedule(epoch, epochs) * lr_scale_extra;
        cur_tau = TauSchedule(epoch, epochs, effective_tau_start, cfg.training.tau_end);

        for (std::size_t start = 0; start < indices.size(); start += batch_size) {
            const std::size_t end =
                std::min(indices.size(), start + static_cast<std::size_t>(batch_size));
            const std::size_t actual = end - start;
            if (actual == 0) continue;

            std::fill(batch_logit_grad.begin(), batch_logit_grad.end(), 0.0);
            std::fill(batch_gauss_grad.begin(), batch_gauss_grad.end(), 0.0);
            for (std::size_t b = start; b < end; ++b) {
                const auto& s = train_samples[indices[b]];
                ForwardPassNas(logits, gaussian_mu, gaussian_sigma, membership,
                                n_rules, n_slots, n_atoms, s.atoms, cur_tau, &cache);
                BackwardPassNas(cache, s.atoms, gaussian_mu, gaussian_sigma, membership,
                                  s.target, n_rules, n_slots, n_atoms, cur_tau,
                                  &single_logit, &single_mu, &single_sigma);
                for (std::size_t k = 0; k < logits.size(); ++k) {
                    batch_logit_grad[k] += single_logit[k];
                }
                for (std::size_t j = 0; j < n_gauss; ++j) {
                    batch_gauss_grad[j] += single_mu[j];
                    batch_gauss_grad[n_gauss + j] += single_sigma[j];
                }
            }
            const double inv = 1.0 / static_cast<double>(actual);
            for (std::size_t k = 0; k < logits.size(); ++k) batch_logit_grad[k] *= inv;
            for (std::size_t k = 0; k < batch_gauss_grad.size(); ++k) batch_gauss_grad[k] *= inv;

            if (cfg.training.lambda_diversity > 0.0) {
                AddDiversityGradient(cache, n_rules, n_slots, n_atoms,
                                       cfg.training.lambda_diversity, cur_tau, &batch_logit_grad);
            }
            opt_logits.Step(logits, batch_logit_grad, lr_scale);

            std::vector<double> gauss_concat(n_gauss * 2);
            for (std::size_t j = 0; j < n_gauss; ++j) {
                gauss_concat[j] = gaussian_mu[j];
                gauss_concat[n_gauss + j] = gaussian_sigma[j];
            }
            opt_gauss.Step(gauss_concat, batch_gauss_grad, lr_scale);
            for (std::size_t j = 0; j < n_gauss; ++j) {
                gaussian_mu[j] = gauss_concat[j];
                gaussian_sigma[j] = std::max(0.05, std::min(1.0, gauss_concat[n_gauss + j]));
            }
        }

        sgd_seconds += std::chrono::duration<double>(
                           std::chrono::steady_clock::now() - sgd_t0)
                           .count();

        ++epochs_done;
        last_train_loss = EvalMseNas(train_samples, logits,
                                     gaussian_mu, gaussian_sigma, membership,
                                     n_rules, n_slots, n_atoms, cur_tau);

        double cur_val_loss = std::numeric_limits<double>::quiet_NaN();
        if (!test_samples.empty()) {
            cur_val_loss = EvalMseNas(test_samples, logits,
                                      gaussian_mu, gaussian_sigma, membership,
                                      n_rules, n_slots, n_atoms, cur_tau);
            if (cur_val_loss < best_val_loss) {
                best_val_loss = cur_val_loss;
                best_logits = logits;
                best_mu = gaussian_mu;
                best_sigma = gaussian_sigma;
            }
        }

        if ((epoch + 1) % 5 == 0 || epoch == 0 || epoch + 1 == epochs) {
            ereport(NOTICE, (errmsg("recdb2/fnn-nas: epoch %d/%d lr=%.4f tau=%.4f train=%.6f val=%.6f",
                                     epoch + 1, epochs,
                                     cfg.training.learning_rate * lr_scale, cur_tau,
                                     last_train_loss, cur_val_loss)));
        }
    }

    ereport(NOTICE, (errmsg("recdb2/fnn-nas: SGD wall-clock %.2f s (%d эпох, серийно, 1 поток)",
                            sgd_seconds, epochs_done)));

    if (!test_samples.empty()) {
        logits = best_logits;
        gaussian_mu = best_mu;
        gaussian_sigma = best_sigma;
    }

    LearnedFnnState state;
    state.n_atoms = n_atoms;
    state.n_rules = n_rules;
    state.n_slots = n_slots;
    state.atoms = atoms;
    state.weights = std::move(logits);
    state.gaussian_mu = std::move(gaussian_mu);
    state.gaussian_sigma = std::move(gaussian_sigma);
    state.metrics.n_train = static_cast<int>(train_samples.size());
    state.metrics.n_test = static_cast<int>(test_samples.size());
    state.metrics.epochs_trained = epochs_done;
    state.metrics.final_train_loss = last_train_loss;

    if (!test_samples.empty()) {
        state.metrics.final_val_loss = best_val_loss;
        // Final eval с tau_end (≈ hard discrete rules)
        auto scored = ScoreSamplesNas(test_samples, state.weights,
                                        state.gaussian_mu, state.gaussian_sigma, membership,
                                        n_rules, n_slots, n_atoms, cfg.training.tau_end);
        FillEvaluationMetrics(scored, &state.metrics);

        ereport(NOTICE,
                (errmsg("recdb2/fnn-nas: best val_loss=%.6f p@5=%.4f r@5=%.4f ndcg@5=%.4f "
                        "p@10=%.4f r@10=%.4f ndcg@10=%.4f",
                        best_val_loss, state.metrics.precision_at_5,
                        state.metrics.recall_at_5, state.metrics.ndcg_at_5,
                        state.metrics.precision_at_10, state.metrics.recall_at_10,
                        state.metrics.ndcg_at_10)));
    }
    return state;
}

}  // namespace

void SplitTrainTest(std::vector<TrainingSample>& all, double test_split, long seed,
                    std::vector<TrainingSample>* train_out,
                    std::vector<TrainingSample>* test_out) {
    std::mt19937_64 gen(static_cast<std::uint64_t>(seed));
    std::shuffle(all.begin(), all.end(), gen);
    const std::size_t n_test =
        static_cast<std::size_t>(std::round(test_split * static_cast<double>(all.size())));
    test_out->assign(all.begin(), all.begin() + n_test);
    train_out->assign(all.begin() + n_test, all.end());
}

LearnedFnnState TrainFnn(const FnnConfig& cfg, const std::vector<AtomDef>& atoms,
                          const std::vector<TrainingSample>& train_samples,
                          const std::vector<TrainingSample>& test_samples,
                          const LearnedFnnState* prev_state) {
    const int n_atoms = static_cast<int>(atoms.size());
    const int n_rules = cfg.training.n_rules;
    if (n_atoms == 0) {
        ereport(ERROR, (errmsg("recdb2/fnn: no atoms configured")));
    }
    if (train_samples.empty()) {
        ereport(ERROR, (errmsg("recdb2/fnn: empty training set")));
    }

    ereport(NOTICE,
            (errmsg("recdb2/fnn: NAS-mode (n_slots=%d, tau %.2f→%.2f)",
                    cfg.training.n_slots, cfg.training.tau_start, cfg.training.tau_end)));
    if (cfg.training.parallel_workers > 1) {
        return TrainFnnParallel(cfg, atoms, train_samples, test_samples, prev_state);
    }
    return TrainFnnNas(cfg, atoms, train_samples, test_samples, prev_state);
}

}  // namespace recdb2::algorithm::fnn
