#pragma once

#include <cstddef>
#include <vector>

namespace recdb2::algorithm::fnn {

double Sigmoid(double x);

struct NasForwardCache {
    std::vector<double> atom_values;
    std::vector<double> probs;
    std::vector<double> slot_values;
    std::vector<double> rule_values;
    std::vector<double> one_minus_rule;
    double prod_one_minus_rule = 1.0;
    double y = 0.0;
};

struct AtomMembership {
    std::uint8_t kind = 0;
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

}
