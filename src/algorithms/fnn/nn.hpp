// Дифференцируемая Fuzzy NN с DARTS-style atom selection (baseline Bartl et al. 2025).
// Каждое правило = K слотов; слот = softmax-смесь атомов. Логиты θ[i,k,j] в одном
// векторе [n_rules * n_slots * n_atoms]; τ отжигается tau_start → tau_end.
//   p[i,k,j] = softmax(θ[i,k,:]/τ);  v[i,k] = Σ_j p·a_j;  R[i] = Π_k v[i,k];  y = 1 − Π_i (1 − R[i])

#pragma once

#include <cstddef>
#include <vector>

namespace recdb2::algorithm::fnn {

double Sigmoid(double x);

struct NasForwardCache {
    std::vector<double> atom_values;          // [n_atoms] — после Gaussian membership
    std::vector<double> probs;                // [n_rules * n_slots * n_atoms]
    std::vector<double> slot_values;          // [n_rules * n_slots]
    std::vector<double> rule_values;          // [n_rules] — product t-norm по слотам
    std::vector<double> one_minus_rule;
    double prod_one_minus_rule = 1.0;
    double y = 0.0;
};

// Маркирует атом который проходит через Gaussian membership (numeric content).
struct AtomMembership {
    std::uint8_t kind = 0;  // 0 = Binary (pass-through), 1 = Gaussian
};

double ForwardPassNas(const std::vector<double>& logits,
                      const std::vector<double>& gaussian_mu,
                      const std::vector<double>& gaussian_sigma,
                      const std::vector<AtomMembership>& membership,
                      int n_rules, int n_slots, int n_atoms,
                      const std::vector<double>& atoms_raw, double tau,
                      NasForwardCache* cache_out);

void BackwardPassNas(const NasForwardCache& cache,
                     const std::vector<double>& atoms_raw,
                     const std::vector<double>& gaussian_mu,
                     const std::vector<double>& gaussian_sigma,
                     const std::vector<AtomMembership>& membership,
                     double target,
                     int n_rules, int n_slots, int n_atoms, double tau,
                     std::vector<double>* logit_grad,
                     std::vector<double>* mu_grad,
                     std::vector<double>* sigma_grad);

// Анти-коллапс: пенализирует ситуацию когда два слота одного правила выбирают
// один и тот же атом. Loss = λ_div · Σ_{i,k1<k2} <p_{i,k1}, p_{i,k2}>.
void AddDiversityGradient(const NasForwardCache& cache, int n_rules, int n_slots, int n_atoms,
                           double lambda_div, double tau,
                           std::vector<double>* logit_grad);

void InitializeLogitsNas(std::vector<double>& logits, int n_rules, int n_slots, int n_atoms,
                         long random_seed);

class AdamOptimizer {
   public:
    AdamOptimizer(std::size_t n_params, double lr, double beta1, double beta2, double eps);

    void Step(std::vector<double>& weights, const std::vector<double>& grad, double lr_scale);

   private:
    double base_lr_;
    double beta1_;
    double beta2_;
    double eps_;
    std::int64_t t_ = 0;
    std::vector<double> m_;
    std::vector<double> v_;
};

}  // namespace recdb2::algorithm::fnn
