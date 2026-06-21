#include "nn.hpp"

#include <cmath>
#include <cstdint>
#include <random>

namespace recdb2::algorithm::fnn {

double Sigmoid(double x) {
    if (x >= 0.0) {
        const double z = std::exp(-x);
        return 1.0 / (1.0 + z);
    }
    const double z = std::exp(x);
    return z / (1.0 + z);
}

AdamOptimizer::AdamOptimizer(std::size_t n_params, double lr, double beta1, double beta2,
                              double eps)
    : base_lr_(lr), beta1_(beta1), beta2_(beta2), eps_(eps), m_(n_params, 0.0),
      v_(n_params, 0.0) {}

void AdamOptimizer::Step(std::vector<double>& weights, const std::vector<double>& grad,
                          double lr_scale) {
    ++t_;
    const double lr = base_lr_ * lr_scale;
    const double bc1 = 1.0 - std::pow(beta1_, static_cast<double>(t_));
    const double bc2 = 1.0 - std::pow(beta2_, static_cast<double>(t_));
    for (std::size_t k = 0; k < weights.size(); ++k) {
        m_[k] = beta1_ * m_[k] + (1.0 - beta1_) * grad[k];
        v_[k] = beta2_ * v_[k] + (1.0 - beta2_) * grad[k] * grad[k];
        const double m_hat = m_[k] / bc1;
        const double v_hat = v_[k] / bc2;
        weights[k] -= lr * m_hat / (std::sqrt(v_hat) + eps_);
    }
}

namespace {

void SoftmaxRow(const double* logits, std::size_t n, double tau, double* probs_out) {
    double max_lv = logits[0] / tau;
    for (std::size_t j = 1; j < n; ++j) {
        const double lv = logits[j] / tau;
        if (lv > max_lv) max_lv = lv;
    }
    double sum = 0.0;
    for (std::size_t j = 0; j < n; ++j) {
        const double e = std::exp(logits[j] / tau - max_lv);
        probs_out[j] = e;
        sum += e;
    }
    const double inv = 1.0 / sum;
    for (std::size_t j = 0; j < n; ++j) probs_out[j] *= inv;
}

}

namespace {

double GaussianMembership(double x, double mu, double sigma) {
    const double s = std::max(0.05, std::fabs(sigma));
    const double z = (x - mu) / s;
    return std::exp(-0.5 * z * z);
}

}

double ForwardPassNas(const std::vector<double>& logits,
                      const std::vector<double>& gaussian_mu,
                      const std::vector<double>& gaussian_sigma,
                      const std::vector<AtomMembership>& membership,
                      int n_rules, int n_slots, int n_atoms,
                      const std::vector<double>& atoms_raw, double tau,
                      NasForwardCache* cache_out) {
    const std::size_t na = static_cast<std::size_t>(n_atoms);
    const std::size_t nr = static_cast<std::size_t>(n_rules);
    const std::size_t ns = static_cast<std::size_t>(n_slots);

    std::vector<double> atom_values(na, 0.0);
    for (std::size_t j = 0; j < na; ++j) {
        if (j < membership.size() && membership[j].kind == 1 &&
            j < gaussian_mu.size() && j < gaussian_sigma.size()) {
            atom_values[j] = GaussianMembership(atoms_raw[j], gaussian_mu[j], gaussian_sigma[j]);
        } else {
            atom_values[j] = atoms_raw[j];
        }
    }

    const std::size_t n_p = nr * ns * na;
    std::vector<double> probs(n_p, 0.0);
    std::vector<double> slot_values(nr * ns, 0.0);

    for (int i = 0; i < n_rules; ++i) {
        for (int k = 0; k < n_slots; ++k) {
            const std::size_t base = (static_cast<std::size_t>(i) * ns + k) * na;
            SoftmaxRow(&logits[base], na, tau, &probs[base]);
            double v = 0.0;
            for (int j = 0; j < n_atoms; ++j) v += probs[base + j] * atom_values[j];
            slot_values[static_cast<std::size_t>(i) * ns + k] = v;
        }
    }

    std::vector<double> rule_values(nr, 0.0);
    for (int i = 0; i < n_rules; ++i) {
        const std::size_t slot_base = static_cast<std::size_t>(i) * ns;
        double prod = 1.0;
        for (int k = 0; k < n_slots; ++k) prod *= slot_values[slot_base + k];
        rule_values[i] = prod;
    }

    std::vector<double> one_minus_rule(nr);
    double prod_inv = 1.0;
    for (int i = 0; i < n_rules; ++i) {
        one_minus_rule[i] = 1.0 - rule_values[i];
        prod_inv *= one_minus_rule[i];
    }
    const double y = 1.0 - prod_inv;

    if (cache_out) {
        cache_out->atom_values = std::move(atom_values);
        cache_out->probs = std::move(probs);
        cache_out->slot_values = std::move(slot_values);
        cache_out->rule_values = std::move(rule_values);
        cache_out->one_minus_rule = std::move(one_minus_rule);
        cache_out->prod_one_minus_rule = prod_inv;
        cache_out->y = y;
    }
    return y;
}

void BackwardPassNas(const NasForwardCache& cache,
                     const std::vector<double>& atoms_raw,
                     const std::vector<double>& gaussian_mu,
                     const std::vector<double>& gaussian_sigma,
                     const std::vector<AtomMembership>& membership,
                     double target,
                     int n_rules, int n_slots, int n_atoms, double tau,
                     std::vector<double>* logit_grad,
                     std::vector<double>* mu_grad,
                     std::vector<double>* sigma_grad) {
    const std::size_t na = static_cast<std::size_t>(n_atoms);
    const std::size_t nr = static_cast<std::size_t>(n_rules);
    const std::size_t ns = static_cast<std::size_t>(n_slots);

    logit_grad->assign(nr * ns * na, 0.0);
    mu_grad->assign(na, 0.0);
    sigma_grad->assign(na, 0.0);

    constexpr double kSafe = 1e-12;
    const double dL_dy = 2.0 * (cache.y - target);

    std::vector<double> dL_dav(na, 0.0);

    for (int i = 0; i < n_rules; ++i) {
        const std::size_t slot_base = static_cast<std::size_t>(i) * ns;

        double dy_dRi;
        const double omr_i = cache.one_minus_rule[i];
        if (omr_i > kSafe) {
            dy_dRi = cache.prod_one_minus_rule / omr_i;
        } else {
            dy_dRi = 1.0;
            for (int m = 0; m < n_rules; ++m) {
                if (m == i) continue;
                dy_dRi *= cache.one_minus_rule[m];
            }
        }
        const double dL_dRi = dL_dy * dy_dRi;
        const double Ri = cache.rule_values[i];

        for (int k = 0; k < n_slots; ++k) {
            const std::size_t slot_idx = slot_base + k;
            const double v = cache.slot_values[slot_idx];

            double dRi_dv;
            if (v > kSafe) {
                dRi_dv = Ri / v;
            } else {
                dRi_dv = 1.0;
                for (int m = 0; m < n_slots; ++m) {
                    if (m == k) continue;
                    dRi_dv *= cache.slot_values[slot_base + m];
                }
            }
            const double dL_dv = dL_dRi * dRi_dv;

            const std::size_t lbase = slot_idx * na;
            for (int j = 0; j < n_atoms; ++j) {
                const double p_j = cache.probs[lbase + j];
                (*logit_grad)[lbase + j] +=
                    dL_dv * p_j * (cache.atom_values[j] - v) / tau;
                dL_dav[j] += dL_dv * p_j;
            }
        }
    }

    for (int j = 0; j < n_atoms; ++j) {
        if (static_cast<std::size_t>(j) >= membership.size() || membership[j].kind != 1) continue;
        if (static_cast<std::size_t>(j) >= gaussian_mu.size() ||
            static_cast<std::size_t>(j) >= gaussian_sigma.size()) continue;
        const double x = atoms_raw[j];
        const double mu = gaussian_mu[j];
        const double sigma_raw = gaussian_sigma[j];
        const double sigma = std::max(0.05, std::fabs(sigma_raw));
        const double dx = x - mu;
        const double a_j = cache.atom_values[j];
        const double da_dmu = a_j * dx / (sigma * sigma);
        const double sign_sigma = (sigma_raw >= 0.0) ? 1.0 : -1.0;
        const double da_dsigma = a_j * (dx * dx) / (sigma * sigma * sigma) * sign_sigma;
        (*mu_grad)[j] = dL_dav[j] * da_dmu;
        (*sigma_grad)[j] = dL_dav[j] * da_dsigma;
    }
}

void InitializeLogitsNas(std::vector<double>& logits, int n_rules, int n_slots, int n_atoms,
                         long random_seed) {
    const std::size_t n_p = static_cast<std::size_t>(n_rules) * n_slots * n_atoms;
    logits.assign(n_p, 0.0);
    std::mt19937_64 gen(static_cast<std::uint64_t>(random_seed));
    std::normal_distribution<double> dist(0.0, 0.5);
    for (std::size_t k = 0; k < n_p; ++k) logits[k] = dist(gen);
}

void AddDiversityGradient(const NasForwardCache& cache, int n_rules, int n_slots, int n_atoms,
                           double lambda_div, double tau,
                           std::vector<double>* logit_grad) {
    if (lambda_div <= 0.0 || n_rules * n_slots <= 1) return;
    const std::size_t na = static_cast<std::size_t>(n_atoms);
    const std::size_t total_slots =
        static_cast<std::size_t>(n_rules) * static_cast<std::size_t>(n_slots);

    std::vector<double> total(na, 0.0);
    for (std::size_t s = 0; s < total_slots; ++s) {
        const std::size_t base = s * na;
        for (std::size_t j = 0; j < na; ++j) total[j] += cache.probs[base + j];
    }

    for (std::size_t s = 0; s < total_slots; ++s) {
        const std::size_t base = s * na;
        double dot = 0.0;
        for (std::size_t j = 0; j < na; ++j) {
            const double so = total[j] - cache.probs[base + j];
            dot += cache.probs[base + j] * so;
        }
        const double coef = lambda_div / tau;
        for (std::size_t l = 0; l < na; ++l) {
            const double so_l = total[l] - cache.probs[base + l];
            (*logit_grad)[base + l] += coef * cache.probs[base + l] * (so_l - dot);
        }
    }
}

}
