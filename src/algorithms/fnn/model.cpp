#include "model.hpp"

#include "src/spi/execute.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <cstdio>

namespace recdb2::algorithm::fnn {

const char* AtomKindName(AtomKind kind) {
    switch (kind) {
        case AtomKind::ItemsAggregate:
            return "items_aggregate";
        case AtomKind::UserAggregate:
            return "user_aggregate";
        case AtomKind::ItemsColumn:
            return "items_column";
        case AtomKind::UserColumn:
            return "user_column";
    }
    return "unknown";
}

AtomKind ParseAtomKind(const std::string& s) {
    if (s == "items_aggregate") return AtomKind::ItemsAggregate;
    if (s == "user_aggregate") return AtomKind::UserAggregate;
    if (s == "items_column") return AtomKind::ItemsColumn;
    if (s == "user_column") return AtomKind::UserColumn;
    ereport(ERROR, (errmsg("recdb2/fnn: unknown atom kind '%s'", s.c_str())));
    __builtin_unreachable();
}

namespace {

std::string FormatNum(double v) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%.10g", v);
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
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
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

std::string SerializeAtom(const AtomDef& a) {
    std::string out = "{\"name\":\"" + EscapeJson(a.name) + "\"";
    out += ",\"kind\":\"" + std::string(AtomKindName(a.kind)) + "\"";
    if (!a.aggregate_fn.empty()) out += ",\"fn\":\"" + EscapeJson(a.aggregate_fn) + "\"";
    if (!a.column.empty()) out += ",\"col\":\"" + EscapeJson(a.column) + "\"";
    out += ",\"threshold\":" + FormatNum(a.threshold);
    out += ",\"scale\":" + FormatNum(a.scale);
    if (a.negate) out += ",\"negate\":true";
    if (!a.categorical_value.empty())
        out += ",\"categorical_value\":\"" + EscapeJson(a.categorical_value) + "\"";
    if (!a.filter_item_column.empty())
        out += ",\"filter_item_column\":\"" + EscapeJson(a.filter_item_column) + "\"";
    if (!a.filter_item_value.empty())
        out += ",\"filter_item_value\":\"" + EscapeJson(a.filter_item_value) + "\"";
    if (!a.filter_item_op.empty())
        out += ",\"filter_item_op\":\"" + EscapeJson(a.filter_item_op) + "\"";
    if (!a.description.empty())
        out += ",\"description\":\"" + EscapeJson(a.description) + "\"";
    if (a.membership == MembershipKind::Gaussian)
        out += ",\"membership\":\"gaussian\"";
    out += "}";
    return out;
}

std::vector<double> ParseDoubleArray(const std::string& json_array_text) {
    static constexpr const char* kQuery =
        "SELECT e::double precision FROM jsonb_array_elements_text($1::jsonb) AS e ";
    auto rs = spi::Execute(kQuery, json_array_text);
    std::vector<double> out;
    out.reserve(rs.Size());
    for (const auto& row : rs) out.push_back(row[0].As<double>());
    return out;
}

}  // namespace

std::string SerializeFnnState(const LearnedFnnState& state) {
    std::string out = "{\"n_rules\":" + std::to_string(state.n_rules);
    out += ",\"n_atoms\":" + std::to_string(state.n_atoms);
    out += ",\"n_slots\":" + std::to_string(state.n_slots);

    out += ",\"atoms\":[";
    for (std::size_t i = 0; i < state.atoms.size(); ++i) {
        if (i > 0) out += ",";
        out += SerializeAtom(state.atoms[i]);
    }
    out += "]";

    out += ",\"weights\":[";
    for (std::size_t i = 0; i < state.weights.size(); ++i) {
        if (i > 0) out += ",";
        out += FormatNum(state.weights[i]);
    }
    out += "]";

    out += ",\"tnorm_logits\":[";
    for (std::size_t i = 0; i < state.tnorm_logits.size(); ++i) {
        if (i > 0) out += ",";
        out += FormatNum(state.tnorm_logits[i]);
    }
    out += "]";

    out += ",\"gaussian_mu\":[";
    for (std::size_t i = 0; i < state.gaussian_mu.size(); ++i) {
        if (i > 0) out += ",";
        out += FormatNum(state.gaussian_mu[i]);
    }
    out += "]";

    out += ",\"gaussian_sigma\":[";
    for (std::size_t i = 0; i < state.gaussian_sigma.size(); ++i) {
        if (i > 0) out += ",";
        out += FormatNum(state.gaussian_sigma[i]);
    }
    out += "]";


    out += ",\"metrics\":{";
    out += "\"n_train\":" + std::to_string(state.metrics.n_train);
    out += ",\"n_test\":" + std::to_string(state.metrics.n_test);
    out += ",\"epochs_trained\":" + std::to_string(state.metrics.epochs_trained);
    out += ",\"final_train_loss\":" + FormatNum(state.metrics.final_train_loss);
    out += ",\"final_val_loss\":" + FormatNum(state.metrics.final_val_loss);
    out += ",\"precision_at_5\":" + FormatNum(state.metrics.precision_at_5);
    out += ",\"recall_at_5\":" + FormatNum(state.metrics.recall_at_5);
    out += ",\"ndcg_at_5\":" + FormatNum(state.metrics.ndcg_at_5);
    out += ",\"map_at_5\":" + FormatNum(state.metrics.map_at_5);
    out += ",\"precision_at_10\":" + FormatNum(state.metrics.precision_at_10);
    out += ",\"recall_at_10\":" + FormatNum(state.metrics.recall_at_10);
    out += ",\"ndcg_at_10\":" + FormatNum(state.metrics.ndcg_at_10);
    out += ",\"map_at_10\":" + FormatNum(state.metrics.map_at_10);
    out += "}";

    out += ",\"trained_at\":\"" + EscapeJson(state.trained_at) + "\"}";
    return out;
}

LearnedFnnState DeserializeFnnState(const std::string& json) {
    LearnedFnnState state;
    if (json.empty() || json == "{}") return state;

    static constexpr const char* kHeaderQuery =
        "SELECT ($1::jsonb ->> 'n_rules')::int, ($1::jsonb ->> 'n_atoms')::int, "
        "       COALESCE(($1::jsonb ->> 'n_slots')::int, 0) ";
    auto hr = spi::Execute(kHeaderQuery, json);
    if (hr.IsEmpty() || hr[0][0].IsNull()) return state;
    state.n_rules = static_cast<int>(hr.SingleRow()[0].As<std::int64_t>());
    state.n_atoms = static_cast<int>(hr.SingleRow()[1].As<std::int64_t>());
    state.n_slots = static_cast<int>(hr.SingleRow()[2].As<std::int64_t>());

    static constexpr const char* kAtomsQuery =
        "SELECT a->>'name', a->>'kind', a->>'fn', a->>'col', "
        "       (a->>'threshold')::double precision, "
        "       (a->>'scale')::double precision, "
        "       COALESCE((a->>'negate')::boolean, false), "
        "       a->>'categorical_value', "
        "       a->>'filter_item_column', "
        "       a->>'description', "
        "       COALESCE(a->>'membership', 'binary'), "
        "       a->>'filter_item_value', "
        "       a->>'filter_item_op' "
        "FROM jsonb_array_elements($1::jsonb -> 'atoms') AS a ";
    auto ars = spi::Execute(kAtomsQuery, json);
    state.atoms.reserve(ars.Size());
    for (const auto& row : ars) {
        AtomDef a;
        a.name = row[0].As<std::string>();
        a.kind = ParseAtomKind(row[1].As<std::string>());
        a.aggregate_fn = row[2].IsNull() ? std::string{} : row[2].As<std::string>();
        a.column = row[3].IsNull() ? std::string{} : row[3].As<std::string>();
        a.threshold = row[4].IsNull() ? 0.0 : row[4].As<double>();
        a.scale = row[5].IsNull() ? 0.0 : row[5].As<double>();
        a.negate = row[6].As<bool>();
        a.categorical_value = row[7].IsNull() ? std::string{} : row[7].As<std::string>();
        a.filter_item_column = row[8].IsNull() ? std::string{} : row[8].As<std::string>();
        a.description = row[9].IsNull() ? std::string{} : row[9].As<std::string>();
        const auto memstr = row[10].As<std::string>();
        a.membership = (memstr == "gaussian") ? MembershipKind::Gaussian : MembershipKind::Binary;
        a.filter_item_value = row[11].IsNull() ? std::string{} : row[11].As<std::string>();
        a.filter_item_op    = row[12].IsNull() ? std::string{} : row[12].As<std::string>();
        state.atoms.push_back(std::move(a));
    }

    static constexpr const char* kWeightsQuery = "SELECT ($1::jsonb -> 'weights')::text ";
    auto wr = spi::Execute(kWeightsQuery, json);
    if (!wr.IsEmpty() && !wr[0][0].IsNull()) {
        state.weights = ParseDoubleArray(wr[0][0].As<std::string>());
    }

    static constexpr const char* kTnormQuery = "SELECT ($1::jsonb -> 'tnorm_logits')::text ";
    auto tr2 = spi::Execute(kTnormQuery, json);
    if (!tr2.IsEmpty() && !tr2[0][0].IsNull()) {
        state.tnorm_logits = ParseDoubleArray(tr2[0][0].As<std::string>());
    }

    static constexpr const char* kMuQuery = "SELECT ($1::jsonb -> 'gaussian_mu')::text ";
    auto mur = spi::Execute(kMuQuery, json);
    if (!mur.IsEmpty() && !mur[0][0].IsNull()) {
        state.gaussian_mu = ParseDoubleArray(mur[0][0].As<std::string>());
    }

    static constexpr const char* kSigmaQuery = "SELECT ($1::jsonb -> 'gaussian_sigma')::text ";
    auto sig = spi::Execute(kSigmaQuery, json);
    if (!sig.IsEmpty() && !sig[0][0].IsNull()) {
        state.gaussian_sigma = ParseDoubleArray(sig[0][0].As<std::string>());
    }


    static constexpr const char* kMetricsQuery =
        "SELECT (m->>'n_train')::int, (m->>'n_test')::int, (m->>'epochs_trained')::int, "
        "       (m->>'final_train_loss')::double precision, "
        "       (m->>'final_val_loss')::double precision, "
        "       (m->>'precision_at_5')::double precision, "
        "       (m->>'recall_at_5')::double precision, "
        "       (m->>'ndcg_at_5')::double precision, "
        "       (m->>'map_at_5')::double precision, "
        "       (m->>'precision_at_10')::double precision, "
        "       (m->>'recall_at_10')::double precision, "
        "       (m->>'ndcg_at_10')::double precision, "
        "       (m->>'map_at_10')::double precision "
        "FROM jsonb_extract_path($1::jsonb, 'metrics') AS m WHERE m IS NOT NULL ";
    auto mr = spi::Execute(kMetricsQuery, json);
    if (!mr.IsEmpty()) {
        const auto& r = mr.SingleRow();
        state.metrics.n_train = static_cast<int>(r[0].As<std::int64_t>());
        state.metrics.n_test = static_cast<int>(r[1].As<std::int64_t>());
        state.metrics.epochs_trained = static_cast<int>(r[2].As<std::int64_t>());
        state.metrics.final_train_loss = r[3].IsNull() ? 0.0 : r[3].As<double>();
        state.metrics.final_val_loss = r[4].IsNull() ? 0.0 : r[4].As<double>();
        state.metrics.precision_at_5 = r[5].IsNull() ? 0.0 : r[5].As<double>();
        state.metrics.recall_at_5 = r[6].IsNull() ? 0.0 : r[6].As<double>();
        state.metrics.ndcg_at_5 = r[7].IsNull() ? 0.0 : r[7].As<double>();
        state.metrics.map_at_5 = r[8].IsNull() ? 0.0 : r[8].As<double>();
        state.metrics.precision_at_10 = r[9].IsNull() ? 0.0 : r[9].As<double>();
        state.metrics.recall_at_10 = r[10].IsNull() ? 0.0 : r[10].As<double>();
        state.metrics.ndcg_at_10 = r[11].IsNull() ? 0.0 : r[11].As<double>();
        state.metrics.map_at_10 = r[12].IsNull() ? 0.0 : r[12].As<double>();
    }

    static constexpr const char* kTrainedAtQuery = "SELECT $1::jsonb ->> 'trained_at' ";
    auto tr = spi::Execute(kTrainedAtQuery, json);
    if (!tr.IsEmpty() && !tr[0][0].IsNull()) state.trained_at = tr[0][0].As<std::string>();

    return state;
}

}  // namespace recdb2::algorithm::fnn
