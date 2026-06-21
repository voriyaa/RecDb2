#include "metrics.hpp"

#include <algorithm>
#include <cmath>

namespace recdb2::algorithm::fnn {

namespace {

struct PerUser {
    std::vector<UserItemScore> items;
};

std::unordered_map<std::int64_t, PerUser> GroupByUser(
    const std::vector<UserItemScore>& predictions) {
    std::unordered_map<std::int64_t, PerUser> by_user;
    for (const auto& p : predictions) {
        by_user[p.user_id].items.push_back(p);
    }
    return by_user;
}

}

TopKMetrics ComputeTopKMetrics(const std::vector<UserItemScore>& predictions, int k) {
    if (k <= 0) return {};

    auto by_user = GroupByUser(predictions);

    double sum_precision = 0.0;
    double sum_recall = 0.0;
    double sum_ndcg = 0.0;
    double sum_map = 0.0;
    int n_users = 0;

    for (auto& [uid, pu] : by_user) {
        int total_relevant = 0;
        for (const auto& it : pu.items) {
            if (it.target >= 0.5) ++total_relevant;
        }
        if (total_relevant == 0) continue;

        std::sort(pu.items.begin(), pu.items.end(),
                  [](const UserItemScore& a, const UserItemScore& b) {
                      return a.score > b.score;
                  });

        const int top_n = std::min<int>(k, static_cast<int>(pu.items.size()));

        int hits_at_k = 0;
        double dcg = 0.0;
        double ap_num = 0.0;
        int hit_so_far = 0;
        for (int r = 0; r < top_n; ++r) {
            const bool relevant = pu.items[r].target >= 0.5;
            if (relevant) {
                ++hits_at_k;
                ++hit_so_far;
                dcg += 1.0 / std::log2(static_cast<double>(r + 2));
                ap_num += static_cast<double>(hit_so_far) / static_cast<double>(r + 1);
            }
        }

        double idcg = 0.0;
        const int ideal_hits = std::min<int>(top_n, total_relevant);
        for (int r = 0; r < ideal_hits; ++r) {
            idcg += 1.0 / std::log2(static_cast<double>(r + 2));
        }

        const double precision = static_cast<double>(hits_at_k) / static_cast<double>(top_n);
        const double recall =
            static_cast<double>(hits_at_k) / static_cast<double>(total_relevant);
        const double ndcg = (idcg > 0.0) ? (dcg / idcg) : 0.0;
        const double ap = ap_num / static_cast<double>(std::min(k, total_relevant));

        sum_precision += precision;
        sum_recall += recall;
        sum_ndcg += ndcg;
        sum_map += ap;
        ++n_users;
    }

    TopKMetrics out;
    if (n_users == 0) return out;
    const double nd = static_cast<double>(n_users);
    out.precision = sum_precision / nd;
    out.recall = sum_recall / nd;
    out.ndcg = sum_ndcg / nd;
    out.map = sum_map / nd;
    return out;
}

void FillEvaluationMetrics(const std::vector<UserItemScore>& test_predictions,
                            TrainingMetrics* out) {
    const auto m5 = ComputeTopKMetrics(test_predictions, 5);
    const auto m10 = ComputeTopKMetrics(test_predictions, 10);
    out->precision_at_5 = m5.precision;
    out->recall_at_5 = m5.recall;
    out->ndcg_at_5 = m5.ndcg;
    out->map_at_5 = m5.map;
    out->precision_at_10 = m10.precision;
    out->recall_at_10 = m10.recall;
    out->ndcg_at_10 = m10.ndcg;
    out->map_at_10 = m10.map;
}

}
