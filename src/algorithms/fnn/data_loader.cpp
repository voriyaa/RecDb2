#include "data_loader.hpp"

#include "src/spi/execute.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

#include <cstdio>
#include <set>

namespace recdb2::algorithm::fnn {

namespace {

std::string SanitizeForColumn(const std::string& s) {
    // Postgres column names: только [a-zA-Z0-9_]. Заменяем остальное на _.
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') || c == '_') {
            out += c;
        } else {
            out += '_';
        }
    }
    if (out.empty()) out = "x";
    return out;
}

std::string StatKey(const std::string& fn, const std::string& col,
                     const std::string& filter_col = "",
                     const std::string& filter_value = "",
                     const std::string& filter_op = "") {
    std::string base = (col == "*") ? fn + "_all" : fn + "_" + col;
    if (!filter_col.empty()) {
        base += "_f_" + filter_col;
        if (!filter_value.empty()) {
            const std::string op_tag =
                filter_op == "<"  ? "lt"  :
                filter_op == "<=" ? "le"  :
                filter_op == ">"  ? "gt"  :
                filter_op == ">=" ? "ge"  : "eq";
            base += "_" + op_tag + "_" + SanitizeForColumn(filter_value);
        }
    }
    return base;
}

struct AggKey {
    std::string fn;
    std::string column;
    std::string filter_item_column;
    std::string filter_item_value;
    std::string filter_item_op;
    bool operator<(const AggKey& o) const {
        if (fn != o.fn) return fn < o.fn;
        if (column != o.column) return column < o.column;
        if (filter_item_column != o.filter_item_column) return filter_item_column < o.filter_item_column;
        if (filter_item_value != o.filter_item_value) return filter_item_value < o.filter_item_value;
        return filter_item_op < o.filter_item_op;
    }
};

std::string FormatNum(double v) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%.10g", v);
    return buf;
}

std::set<AggKey> CollectAggregateKeys(const std::vector<AtomDef>& atoms, AtomKind kind) {
    std::set<AggKey> keys;
    for (const auto& a : atoms) {
        if (a.kind == kind) keys.insert({a.aggregate_fn, a.column, a.filter_item_column,
                                          a.filter_item_value, a.filter_item_op});
    }
    return keys;
}

std::string SqlEscapeLiteral(const std::string& s);

std::string BuildItemFilterPredicate(const std::string& alias,
                                       const std::string& column,
                                       const std::string& value,
                                       const std::string& op) {
    // пустой value → boolean true
    if (value.empty()) {
        return "(" + alias + "." + column + ")::text = 'true'";
    }
    const std::string actual_op = op.empty() ? "=" : op;
    if (actual_op == "=") {
        return alias + "." + column + " = " + SqlEscapeLiteral(value);  // экранируем литерал
    }
    // value сюда пишет только наш генератор (число) — splice безопасен.
    return "(" + alias + "." + column + ")::double precision " + actual_op + " " + value;
}

std::set<std::string> CollectColumnKeys(const std::vector<AtomDef>& atoms, AtomKind kind) {
    // Numeric column atoms (continuous, threshold) — кастуются в double precision в stats view.
    std::set<std::string> keys;
    for (const auto& a : atoms) {
        if (a.kind == kind && a.categorical_value.empty()) keys.insert(a.column);
    }
    return keys;
}

std::set<std::string> CollectCategoricalKeys(const std::vector<AtomDef>& atoms,
                                               AtomKind kind) {
    // Categorical column atoms — переносятся в stats view как text без cast.
    std::set<std::string> keys;
    for (const auto& a : atoms) {
        if (a.kind == kind && !a.categorical_value.empty()) keys.insert(a.column);
    }
    return keys;
}

std::string SqlEscapeLiteral(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    out += '\'';
    for (char c : s) {
        if (c == '\'') out += '\'';
        out += c;
    }
    out += '\'';
    return out;
}

std::string AtomValueExpression(const AtomDef& a, const std::string& items_alias,
                                  const std::string& users_alias) {
    // Categorical атом: проверка col = 'value'. Не требует numeric.
    if (!a.categorical_value.empty()) {
        std::string raw_col;
        switch (a.kind) {
            case AtomKind::ItemsColumn:
                raw_col = items_alias + "." + a.column;
                break;
            case AtomKind::UserColumn:
                raw_col = users_alias + "." + a.column;
                break;
            default:
                raw_col = items_alias + "." + a.column;
        }
        const std::string literal = SqlEscapeLiteral(a.categorical_value);
        return "(CASE WHEN " + raw_col + "::text = " + literal + " THEN 1.0 ELSE 0.0 END)";
    }

    std::string value;
    switch (a.kind) {
        case AtomKind::ItemsAggregate:
            value = items_alias + "." + StatKey(a.aggregate_fn, a.column, a.filter_item_column,
                                                  a.filter_item_value, a.filter_item_op);
            break;
        case AtomKind::UserAggregate:
            value = users_alias + "." + StatKey(a.aggregate_fn, a.column, a.filter_item_column,
                                                  a.filter_item_value, a.filter_item_op);
            break;
        case AtomKind::ItemsColumn:
            value = items_alias + "." + a.column + "::double precision";
            break;
        case AtomKind::UserColumn:
            value = users_alias + "." + a.column + "::double precision";
            break;
    }
    // Continuous: scale>0 => нормализуем в [0,1] по min/max; NULL → 0.5 (нейтрально).
    if (a.scale > 0.0) {
        const std::string min_v = FormatNum(a.threshold);
        const std::string scale_v = FormatNum(a.scale);
        return "COALESCE(GREATEST(0.0, LEAST(1.0, (" + value + " - " + min_v + ") / " +
                scale_v + ")), 0.5)";
    }
    // Threshold-based: NULL → 0 (например user_likes_X без оценок X) — «признак не подтверждён».
    const std::string cmp = a.negate ? " < " : " >= ";
    return "COALESCE(CASE WHEN " + value + cmp + FormatNum(a.threshold) +
            " THEN 1.0 ELSE 0.0 END, 0.0)";
}

std::string BuildItemStatsSql(const FnnConfig& cfg, const std::vector<AtomDef>& atoms,
                                const std::string& view_name) {
    const auto agg_keys = CollectAggregateKeys(atoms, AtomKind::ItemsAggregate);
    const auto col_keys = CollectColumnKeys(atoms, AtomKind::ItemsColumn);
    const auto cat_keys = CollectCategoricalKeys(atoms, AtomKind::ItemsColumn);

    std::string sql = "CREATE MATERIALIZED VIEW " + view_name + " AS SELECT ";

    if (cfg.items.has_value() && (!col_keys.empty() || !cat_keys.empty())) {
        sql += "i." + cfg.items->id_col + " AS id";
        for (const auto& col : col_keys) {
            sql += ", COALESCE(CASE WHEN (i." + col + ")::text = 'true' THEN 1.0 "
                   "WHEN (i." + col + ")::text = 'false' THEN 0.0 END, "
                   "NULLIF((i." + col + ")::text, '')::double precision) AS " + col;
        }
        for (const auto& col : cat_keys) {
            if (col_keys.count(col)) continue;
            sql += ", i." + col + "::text AS " + col;
        }
        for (const auto& k : agg_keys) {
            const std::string inner = (k.column == "*")
                                           ? "count(*)::double precision"
                                           : k.fn + "(r." + k.column + ")::double precision";
            sql += ", (SELECT " + inner + " FROM " + cfg.interactions.table + " r WHERE r." +
                   cfg.interactions.item_col + " = i." + cfg.items->id_col + ") AS " +
                   StatKey(k.fn, k.column);
        }
        sql += " FROM " + cfg.items->table + " i";
    } else {
        sql += "r." + cfg.interactions.item_col + " AS id";
        for (const auto& k : agg_keys) {
            const std::string inner = (k.column == "*")
                                           ? "count(*)::double precision"
                                           : k.fn + "(r." + k.column + ")::double precision";
            sql += ", " + inner + " AS " + StatKey(k.fn, k.column);
        }
        sql += " FROM " + cfg.interactions.table + " r GROUP BY r." + cfg.interactions.item_col;
    }
    return sql;
}

std::string BuildUserStatsSql(const FnnConfig& cfg, const std::vector<AtomDef>& atoms,
                                const std::string& view_name) {
    const auto agg_keys = CollectAggregateKeys(atoms, AtomKind::UserAggregate);
    const auto col_keys = CollectColumnKeys(atoms, AtomKind::UserColumn);
    const auto cat_keys = CollectCategoricalKeys(atoms, AtomKind::UserColumn);

    bool has_filter_agg = false;
    for (const auto& k : agg_keys) {
        if (!k.filter_item_column.empty()) {
            has_filter_agg = true;
            break;
        }
    }

    if (agg_keys.empty() && col_keys.empty() && cat_keys.empty()) {
        return "CREATE MATERIALIZED VIEW " + view_name + " AS SELECT DISTINCT r." +
               cfg.interactions.user_col + " AS id FROM " + cfg.interactions.table + " r";
    }

    auto inner_agg = [](const AggKey& k) {
        return (k.column == "*") ? std::string{"count(*)::double precision"}
                                  : k.fn + "(r." + k.column + ")::double precision";
    };

    std::string sql = "CREATE MATERIALIZED VIEW " + view_name + " AS SELECT ";
    if (cfg.users.has_value() && (!col_keys.empty() || !cat_keys.empty())) {
        sql += "u." + cfg.users->id_col + " AS id";
        for (const auto& col : col_keys) {
            sql += ", COALESCE(CASE WHEN (u." + col + ")::text = 'true' THEN 1.0 "
                   "WHEN (u." + col + ")::text = 'false' THEN 0.0 END, "
                   "NULLIF((u." + col + ")::text, '')::double precision) AS " + col;
        }
        for (const auto& col : cat_keys) {
            if (col_keys.count(col)) continue;
            sql += ", u." + col + "::text AS " + col;
        }
        for (const auto& k : agg_keys) {
            std::string subq = "(SELECT " + inner_agg(k) + " FROM " +
                                cfg.interactions.table + " r";
            if (!k.filter_item_column.empty() && cfg.items.has_value()) {
                const std::string pred = BuildItemFilterPredicate("m", k.filter_item_column,
                                                                     k.filter_item_value,
                                                                     k.filter_item_op);
                subq += " JOIN " + cfg.items->table + " m ON m." + cfg.items->id_col +
                         " = r." + cfg.interactions.item_col + " AND " + pred;
            }
            subq += " WHERE r." + cfg.interactions.user_col + " = u." +
                     cfg.users->id_col + ")";
            sql += ", " + subq + " AS " + StatKey(k.fn, k.column, k.filter_item_column,
                                                    k.filter_item_value, k.filter_item_op);
        }
        sql += " FROM " + cfg.users->table + " u";
    } else {
        if (has_filter_agg && cfg.items.has_value()) {
            sql += "u.id AS id";
            for (const auto& k : agg_keys) {
                std::string subq = "(SELECT " + inner_agg(k) + " FROM " +
                                    cfg.interactions.table + " r";
                if (!k.filter_item_column.empty()) {
                    const std::string pred = BuildItemFilterPredicate("m", k.filter_item_column,
                                                                         k.filter_item_value,
                                                                         k.filter_item_op);
                    subq += " JOIN " + cfg.items->table + " m ON m." + cfg.items->id_col +
                             " = r." + cfg.interactions.item_col + " AND " + pred;
                }
                subq += " WHERE r." + cfg.interactions.user_col + " = u.id)";
                sql += ", " + subq + " AS " + StatKey(k.fn, k.column, k.filter_item_column,
                                                        k.filter_item_value, k.filter_item_op);
            }
            sql += " FROM (SELECT DISTINCT " + cfg.interactions.user_col + " AS id FROM " +
                    cfg.interactions.table + ") u";
        } else {
            sql += "r." + cfg.interactions.user_col + " AS id";
            for (const auto& k : agg_keys) {
                sql += ", " + inner_agg(k) + " AS " + StatKey(k.fn, k.column);
            }
            sql += " FROM " + cfg.interactions.table + " r GROUP BY r." +
                    cfg.interactions.user_col;
        }
    }
    return sql;
}

}  // namespace

void CreateStatsViews(const FnnConfig& cfg, const std::vector<AtomDef>& atoms,
                       const std::string& item_stats_view, const std::string& user_stats_view) {
    spi::Execute(("DROP MATERIALIZED VIEW IF EXISTS " + item_stats_view + " CASCADE").c_str());
    spi::Execute(BuildItemStatsSql(cfg, atoms, item_stats_view).c_str());
    spi::Execute(("CREATE UNIQUE INDEX " + item_stats_view + "_id_idx ON " + item_stats_view +
                  "(id)")
                     .c_str());
    spi::Execute(("ANALYZE " + item_stats_view).c_str());

    spi::Execute(("DROP MATERIALIZED VIEW IF EXISTS " + user_stats_view + " CASCADE").c_str());
    spi::Execute(BuildUserStatsSql(cfg, atoms, user_stats_view).c_str());
    spi::Execute(("CREATE UNIQUE INDEX " + user_stats_view + "_id_idx ON " + user_stats_view +
                  "(id)")
                     .c_str());
    spi::Execute(("ANALYZE " + user_stats_view).c_str());
}

void LoadDistinctUserItemIds(const FnnConfig& cfg, std::vector<std::int64_t>* user_ids,
                              std::vector<std::int64_t>* item_ids) {
    const std::string user_sql = "SELECT DISTINCT " + cfg.interactions.user_col +
                                  "::bigint FROM " + cfg.interactions.table + " ORDER BY 1";
    auto urs = spi::Execute(user_sql.c_str());
    user_ids->clear();
    user_ids->reserve(urs.Size());
    for (const auto& row : urs) user_ids->push_back(row[0].As<std::int64_t>());

    const std::string item_sql = "SELECT DISTINCT " + cfg.interactions.item_col +
                                  "::bigint FROM " + cfg.interactions.table + " ORDER BY 1";
    auto irs = spi::Execute(item_sql.c_str());
    item_ids->clear();
    item_ids->reserve(irs.Size());
    for (const auto& row : irs) item_ids->push_back(row[0].As<std::int64_t>());
}

void DropStatsViews(const std::string& item_stats_view, const std::string& user_stats_view) {
    spi::Execute(("DROP MATERIALIZED VIEW IF EXISTS " + item_stats_view + " CASCADE").c_str());
    spi::Execute(("DROP MATERIALIZED VIEW IF EXISTS " + user_stats_view + " CASCADE").c_str());
}

std::vector<TrainingSample> LoadTrainingSet(const FnnConfig& cfg,
                                             const std::vector<AtomDef>& atoms,
                                             const std::string& item_stats_view,
                                             const std::string& user_stats_view) {
    const auto& uc = cfg.interactions.user_col;
    const auto& ic = cfg.interactions.item_col;
    const auto& rc = cfg.interactions.rating_col;
    const auto& tc = cfg.interactions.ts_col;
    const auto& it = cfg.interactions.table;

    // Target = per-user PERCENT_RANK ∈ [0,1] (Beel et al. RecSys 2019, «Flatter is Better»):
    // матчится с probabilistic OR, сохраняет ordinal. Вырожденные юзеры (n=1 или min=max) → 0.5.
    std::string prefix_cte = "WITH _ranked AS (SELECT " + uc + ", " + ic + ", " + rc;
    if (!tc.empty()) prefix_cte += ", " + tc;
    prefix_cte += ", PERCENT_RANK() OVER (PARTITION BY " + uc + " ORDER BY " + rc +
                  ") AS _pct"
                  ", COUNT(*) OVER (PARTITION BY " + uc + ") AS _n"
                  ", MIN(" + rc + ") OVER (PARTITION BY " + uc + ") AS _rmin"
                  ", MAX(" + rc + ") OVER (PARTITION BY " + uc + ") AS _rmax"
                  " FROM " + it + ") ";

    std::string sql = prefix_cte + "SELECT r." + uc + ", r." + ic +
                      ", CASE WHEN r._n = 1 OR r._rmin = r._rmax THEN 0.5 ELSE r._pct END";

    for (const auto& a : atoms) {
        sql += ", " + AtomValueExpression(a, "i", "u");
    }

    sql += " FROM _ranked r JOIN " + item_stats_view + " i ON i.id = r." + ic +
           " JOIN " + user_stats_view + " u ON u.id = r." + uc;
    if (!tc.empty()) {
        sql += " ORDER BY r." + tc + " DESC NULLS LAST";
    } else {
        sql += " ORDER BY r." + uc + ", r." + ic;
    }
    sql += " LIMIT $1";

    auto rs = spi::Execute(sql.c_str(),
                            static_cast<std::int64_t>(cfg.training.max_train_samples));

    std::vector<TrainingSample> out;
    out.reserve(rs.Size());
    const std::size_t n_atoms = atoms.size();

    for (const auto& row : rs) {
        TrainingSample s;
        s.user_id = row[0].As<std::int64_t>();
        s.item_id = row[1].As<std::int64_t>();
        s.target = row[2].As<double>();
        s.atoms.reserve(n_atoms);
        bool any_null = false;
        for (std::size_t i = 0; i < n_atoms; ++i) {
            const auto& f = row[3 + i];
            if (f.IsNull()) {
                any_null = true;
                break;
            }
            s.atoms.push_back(f.As<double>());
        }
        if (any_null) continue;
        out.push_back(std::move(s));
    }
    return out;
}

std::vector<InferenceItem> LoadInferenceItems(const FnnConfig& cfg,
                                               const std::vector<AtomDef>& atoms,
                                               const std::string& item_stats_view,
                                               const std::string& user_stats_view,
                                               std::int64_t user_id, bool exclude_rated) {
    std::string sql = "SELECT i.id";
    for (const auto& a : atoms) {
        sql += ", " + AtomValueExpression(a, "i", "u");
    }
    sql += " FROM " + item_stats_view + " i JOIN " + user_stats_view + " u ON u.id = $1";
    if (exclude_rated) {
        sql += " WHERE NOT EXISTS(SELECT 1 FROM " + cfg.interactions.table + " r WHERE r." +
               cfg.interactions.user_col + " = $1 AND r." + cfg.interactions.item_col +
               " = i.id)";
    }

    auto rs = spi::Execute(sql.c_str(), user_id);

    std::vector<InferenceItem> out;
    out.reserve(rs.Size());
    const std::size_t n_atoms = atoms.size();

    for (const auto& row : rs) {
        if (row[0].IsNull()) continue;
        InferenceItem it;
        it.item_id = row[0].As<std::int64_t>();
        it.atoms.reserve(n_atoms);
        bool any_null = false;
        for (std::size_t i = 0; i < n_atoms; ++i) {
            const auto& f = row[1 + i];
            if (f.IsNull()) {
                any_null = true;
                break;
            }
            it.atoms.push_back(f.As<double>());
        }
        if (any_null) continue;
        out.push_back(std::move(it));
    }
    return out;
}

std::optional<InferenceItem> LoadSingleInferenceItem(const FnnConfig& cfg,
                                                     const std::vector<AtomDef>& atoms,
                                                     const std::string& item_stats_view,
                                                     const std::string& user_stats_view,
                                                     std::int64_t user_id,
                                                     std::int64_t item_id) {
    std::string sql = "SELECT i.id";
    for (const auto& a : atoms) {
        sql += ", " + AtomValueExpression(a, "i", "u");
    }
    sql += " FROM " + item_stats_view + " i JOIN " + user_stats_view +
           " u ON u.id = $1 WHERE i.id = $2";

    auto rs = spi::Execute(sql.c_str(), user_id, item_id);
    if (rs.IsEmpty()) return std::nullopt;
    const auto& row = rs.SingleRow();
    if (row[0].IsNull()) return std::nullopt;

    InferenceItem it;
    it.item_id = row[0].As<std::int64_t>();
    it.atoms.reserve(atoms.size());
    for (std::size_t i = 0; i < atoms.size(); ++i) {
        if (row[1 + i].IsNull()) return std::nullopt;
        it.atoms.push_back(row[1 + i].As<double>());
    }
    return it;
}

}  // namespace recdb2::algorithm::fnn
