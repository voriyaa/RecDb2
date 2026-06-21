#include "training_support.hpp"

#include <algorithm>
#include <cmath>

namespace recdb2::algorithm::fnn {

std::vector<AtomMembership> MakeMembership(const std::vector<AtomDef>& atoms) {
    std::vector<AtomMembership> out(atoms.size());
    for (std::size_t i = 0; i < atoms.size(); ++i) {
        out[i].kind = (atoms[i].membership == MembershipKind::Gaussian) ? 1 : 0;
    }
    return out;
}

double LrSchedule(int epoch, int total_epochs) {
    const int warmup = std::min(3, total_epochs / 10);
    if (epoch < warmup) {
        return static_cast<double>(epoch + 1) / static_cast<double>(warmup + 1);
    }
    const double progress = static_cast<double>(epoch - warmup) /
                            static_cast<double>(std::max(1, total_epochs - warmup));
    return 0.5 * (1.0 + std::cos(M_PI * progress));
}

double TauSchedule(int epoch, int total_epochs, double tau_start, double tau_end) {
    if (total_epochs <= 1) return tau_end;
    const double t = static_cast<double>(epoch) / static_cast<double>(total_epochs - 1);
    return tau_start * std::pow(tau_end / tau_start, t);  // экспоненциальный спад
}

double EvalMseNas(const std::vector<TrainingSample>& samples, const std::vector<double>& logits,
                  const std::vector<double>& mu, const std::vector<double>& sigma,
                  const std::vector<AtomMembership>& membership, int n_rules, int n_slots,
                  int n_atoms, double tau) {
    if (samples.empty()) return 0.0;
    double total = 0.0;
    NasForwardCache cache;
    for (const auto& s : samples) {
        const double y = ForwardPassNas(logits, mu, sigma, membership, n_rules, n_slots, n_atoms,
                                        s.atoms, tau, &cache);
        const double e = y - s.target;
        total += e * e;
    }
    return total / static_cast<double>(samples.size());
}

std::vector<UserItemScore> ScoreSamplesNas(const std::vector<TrainingSample>& samples,
                                           const std::vector<double>& logits,
                                           const std::vector<double>& mu,
                                           const std::vector<double>& sigma,
                                           const std::vector<AtomMembership>& membership,
                                           int n_rules, int n_slots, int n_atoms, double tau) {
    std::vector<UserItemScore> out;
    out.reserve(samples.size());
    NasForwardCache cache;
    for (const auto& s : samples) {
        const double y = ForwardPassNas(logits, mu, sigma, membership, n_rules, n_slots, n_atoms,
                                        s.atoms, tau, &cache);
        out.push_back({s.user_id, s.item_id, y, s.target});
    }
    return out;
}

}  // namespace recdb2::algorithm::fnn
