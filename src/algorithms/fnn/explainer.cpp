#include "explainer.hpp"

#include "nn.hpp"

#include <algorithm>
#include <cstdio>
#include <string>

namespace recdb2::algorithm::fnn {

namespace {

std::string FormatNum(double v) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%.6g", v);
    return buf;
}

std::string EscapeJson(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    for (char c : s) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    snprintf(buf, sizeof(buf), "\\u%04x",
                             static_cast<unsigned int>(static_cast<unsigned char>(c)));
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    return out;
}

std::string AtomLabel(const AtomDef& a) {
    if (!a.categorical_value.empty()) {
        return a.name + " (" + a.column + " = '" + a.categorical_value + "')";
    }
    std::string label = a.name;
    label += " (";
    switch (a.kind) {
        case AtomKind::ItemsAggregate:
        case AtomKind::UserAggregate:
            label += a.aggregate_fn;
            if (a.column != "*") label += "(" + a.column + ")";
            else label += "(*)";
            break;
        case AtomKind::ItemsColumn:
        case AtomKind::UserColumn:
            label += a.column;
            break;
    }
    if (a.scale > 0.0) {
        label += " in [" + FormatNum(a.threshold) + ".." +
                 FormatNum(a.threshold + a.scale) + "])";
    } else {
        label += " >= " + FormatNum(a.threshold) + ")";
    }
    return label;
}

}

std::vector<ExplanationItem> ExplainPrediction(const LearnedFnnState& state,
                                                const InferenceItem& sample,
                                                std::int64_t) {
    std::vector<ExplanationItem> out;

    NasForwardCache cache;
    std::vector<AtomMembership> membership(state.atoms.size());
    for (std::size_t i = 0; i < state.atoms.size(); ++i) {
        membership[i].kind =
            (state.atoms[i].membership == MembershipKind::Gaussian) ? 1 : 0;
    }
    const double y = ForwardPassNas(state.weights, state.gaussian_mu, state.gaussian_sigma,
                                    membership, state.n_rules, state.n_slots,
                                    state.n_atoms, sample.atoms, 0.1, &cache);
    out.push_back(ExplanationItem{
        .kind = "prediction",
        .label = "fnn_score",
        .contribution = y,
        .details_json = "{\"item_id\":" + std::to_string(sample.item_id) +
                         ",\"n_slots\":" + std::to_string(state.n_slots) + "}",
    });
    for (int i = 0; i < state.n_rules; ++i) {
        std::string details = "{\"slots\":[";
        for (int k = 0; k < state.n_slots; ++k) {
            if (k > 0) details += ",";
            const std::size_t base =
                (static_cast<std::size_t>(i) * state.n_slots + k) * state.n_atoms;
            int argmax = 0;
            double pmax = cache.probs[base];
            for (int j = 1; j < state.n_atoms; ++j) {
                if (cache.probs[base + j] > pmax) {
                    pmax = cache.probs[base + j];
                    argmax = j;
                }
            }
            details += "{\"slot\":" + std::to_string(k + 1) +
                        ",\"atom\":\"" + EscapeJson(state.atoms[argmax].name) +
                        "\",\"prob\":" + FormatNum(pmax) +
                        ",\"value\":" + FormatNum(sample.atoms[argmax]) + "}";
        }
        details += "]}";
        out.push_back(ExplanationItem{
            .kind = "rule",
            .label = "rule_" + std::to_string(i + 1),
            .contribution = cache.rule_values[i],
            .details_json = std::move(details),
        });
    }
    return out;
}

std::vector<ExplanationItem> IntrospectModel(const LearnedFnnState& state) {
    std::vector<ExplanationItem> out;
    if (state.n_atoms == 0) return out;

    out.push_back(ExplanationItem{
        .kind = "summary",
        .label = "config",
        .contribution = 0.0,
        .details_json = "{\"n_rules\":" + std::to_string(state.n_rules) +
                        ",\"n_atoms\":" + std::to_string(state.n_atoms) +
                        ",\"n_train\":" + std::to_string(state.metrics.n_train) +
                        ",\"n_test\":" + std::to_string(state.metrics.n_test) +
                        ",\"epochs_trained\":" + std::to_string(state.metrics.epochs_trained) +
                        "}",
    });

    out.push_back(ExplanationItem{
        .kind = "metric",
        .label = "final_train_loss",
        .contribution = state.metrics.final_train_loss,
        .details_json = "{}",
    });
    out.push_back(ExplanationItem{
        .kind = "metric",
        .label = "final_val_loss",
        .contribution = state.metrics.final_val_loss,
        .details_json = "{}",
    });
    out.push_back(ExplanationItem{
        .kind = "metric",
        .label = "precision_at_5",
        .contribution = state.metrics.precision_at_5,
        .details_json = "{}",
    });
    out.push_back(ExplanationItem{
        .kind = "metric",
        .label = "recall_at_5",
        .contribution = state.metrics.recall_at_5,
        .details_json = "{}",
    });
    out.push_back(ExplanationItem{
        .kind = "metric",
        .label = "ndcg_at_5",
        .contribution = state.metrics.ndcg_at_5,
        .details_json = "{}",
    });
    out.push_back(ExplanationItem{
        .kind = "metric",
        .label = "map_at_5",
        .contribution = state.metrics.map_at_5,
        .details_json = "{}",
    });
    out.push_back(ExplanationItem{
        .kind = "metric",
        .label = "precision_at_10",
        .contribution = state.metrics.precision_at_10,
        .details_json = "{}",
    });
    out.push_back(ExplanationItem{
        .kind = "metric",
        .label = "recall_at_10",
        .contribution = state.metrics.recall_at_10,
        .details_json = "{}",
    });
    out.push_back(ExplanationItem{
        .kind = "metric",
        .label = "ndcg_at_10",
        .contribution = state.metrics.ndcg_at_10,
        .details_json = "{}",
    });
    out.push_back(ExplanationItem{
        .kind = "metric",
        .label = "map_at_10",
        .contribution = state.metrics.map_at_10,
        .details_json = "{}",
    });

    for (std::size_t j = 0; j < state.atoms.size(); ++j) {
        const auto& a = state.atoms[j];
        double total_w = 0.0;
        std::string per_rule = "[";
        for (int i = 0; i < state.n_rules; ++i) {
            const double w_fuzz = Sigmoid(state.W(i, static_cast<int>(j)));
            total_w += w_fuzz;
            if (i > 0) per_rule += ",";
            per_rule += FormatNum(w_fuzz);
        }
        per_rule += "]";

        std::string details = "{\"kind\":\"" + std::string(AtomKindName(a.kind)) +
                              "\",\"threshold\":" + FormatNum(a.threshold) +
                              ",\"per_rule_weights\":" + per_rule + "}";
        out.push_back(ExplanationItem{
            .kind = "atom",
            .label = AtomLabel(a),
            .contribution = total_w / std::max(1, state.n_rules),
            .details_json = std::move(details),
        });
    }

    for (int i = 0; i < state.n_rules; ++i) {
        std::string body = "{\"weights\":[";
        for (int j = 0; j < state.n_atoms; ++j) {
            if (j > 0) body += ",";
            body += "{\"atom\":\"" + EscapeJson(state.atoms[j].name) +
                    "\",\"fuzzy_weight\":" + FormatNum(Sigmoid(state.W(i, j))) + "}";
        }
        body += "]}";
        out.push_back(ExplanationItem{
            .kind = "rule",
            .label = "rule_" + std::to_string(i + 1),
            .contribution = 0.0,
            .details_json = std::move(body),
        });
    }

    return out;
}

}
