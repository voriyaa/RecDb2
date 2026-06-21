#include "atom_builder.hpp"

#include "src/spi/execute.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

namespace recdb2::algorithm::fnn {

namespace {

struct Quartiles {
    bool valid = false;
    double p33 = 0.0;
    double p66 = 0.0;
};

Quartiles ColumnQuartiles(const std::string& table, const std::string& column) {
    // Используем только реальные числовые типы (boolean уходит в другой scan).
    const std::string sql =
        "SELECT percentile_cont(0.33) WITHIN GROUP (ORDER BY (" + column +
        ")::double precision), "
        "percentile_cont(0.66) WITHIN GROUP (ORDER BY (" + column +
        ")::double precision) "
        "FROM " + table + " WHERE " + column + " IS NOT NULL";
    auto rs = spi::Execute(sql.c_str());
    Quartiles q;
    if (rs.IsEmpty() || rs[0][0].IsNull() || rs[0][1].IsNull()) return q;
    q.p33 = rs[0][0].As<double>();
    q.p66 = rs[0][1].As<double>();
    if (q.p33 == q.p66) return q;  // вырожденная колонка
    q.valid = true;
    return q;
}

std::pair<double, double> GetColumnRange(const std::string& table, const std::string& column) {
    // Сначала достаём массив значений, кастуя через text — чтобы работало и для boolean.
    const std::string expr = "COALESCE(CASE WHEN (" + column +
                             ")::text = 'true' THEN 1.0 WHEN (" + column +
                             ")::text = 'false' THEN 0.0 END, NULLIF((" + column +
                             ")::text, '')::double precision)";
    const std::string sql = "SELECT min(" + expr + "), max(" + expr + ") FROM " + table +
                            " WHERE " + column + " IS NOT NULL";
    auto rs = spi::Execute(sql.c_str());
    if (rs.IsEmpty() || rs[0][0].IsNull() || rs[0][1].IsNull()) {
        return {0.0, 1.0};
    }
    return {rs[0][0].As<double>(), rs[0][1].As<double>()};
}

std::pair<double, double> GetAggregateRange(const FnnConfig& cfg, const std::string& agg_fn,
                                              const std::string& col, AtomKind kind) {
    const std::string group_col = (kind == AtomKind::ItemsAggregate)
                                       ? cfg.interactions.item_col
                                       : cfg.interactions.user_col;
    const std::string inner = (col == "*")
                                   ? std::string{"count(*)::double precision"}
                                   : agg_fn + "(" + col + ")::double precision";
    const std::string sql = "SELECT min(agg_v), max(agg_v) FROM (SELECT " + inner +
                            " AS agg_v FROM " + cfg.interactions.table + " GROUP BY " +
                            group_col + ") sub WHERE agg_v IS NOT NULL";
    auto rs = spi::Execute(sql.c_str());
    if (rs.IsEmpty() || rs[0][0].IsNull() || rs[0][1].IsNull()) {
        return {0.0, 1.0};
    }
    return {rs[0][0].As<double>(), rs[0][1].As<double>()};
}

struct CategoricalValue {
    std::string column;
    std::string value;
    std::int64_t count;
};

std::int64_t ColumnDistinctCount(const std::string& table, const std::string& col) {
    const std::string sql = "SELECT COUNT(DISTINCT " + col + ")::bigint FROM " + table;
    auto rs = spi::Execute(sql.c_str());
    if (rs.IsEmpty() || rs[0][0].IsNull()) return 0;
    return rs[0][0].As<std::int64_t>();
}

std::int64_t TableRowCount(const std::string& table) {
    const std::string sql = "SELECT COUNT(*)::bigint FROM " + table;
    auto rs = spi::Execute(sql.c_str());
    if (rs.IsEmpty() || rs[0][0].IsNull()) return 0;
    return rs[0][0].As<std::int64_t>();
}

std::vector<std::string> ScanCategoricalColumns(const std::string& table,
                                                  const std::string& id_col_to_skip) {
    static constexpr const char* kQuery =
        "SELECT a.attname "
        "FROM pg_attribute a "
        "WHERE a.attrelid = $1::regclass "
        "  AND a.attnum > 0 "
        "  AND NOT a.attisdropped "
        "  AND format_type(a.atttypid, a.atttypmod) IN ('text', 'character varying') "
        "ORDER BY a.attnum";
    auto rs = spi::Execute(kQuery, table);
    const std::int64_t rows = TableRowCount(table);
    // Identifier-like text колонки (title, name, isbn, url) скипаем — их
    // top-K даёт уникальные значения = мусор. Эвристика: cardinality > 30% rows.
    const std::int64_t max_card =
        std::max<std::int64_t>(50, rows / 3);
    std::vector<std::string> out;
    for (const auto& row : rs) {
        const auto name = row[0].As<std::string>();
        if (name == id_col_to_skip) continue;
        if (ColumnDistinctCount(table, name) > max_card) continue;
        out.push_back(name);
    }
    return out;
}

std::vector<CategoricalValue> GetTopValues(const std::string& table, const std::string& column,
                                            int top_k) {
    const std::string sql = "SELECT " + column + "::text AS v, count(*)::bigint AS cnt FROM " +
                            table + " WHERE " + column + " IS NOT NULL GROUP BY " + column +
                            " ORDER BY cnt DESC LIMIT $1";
    auto rs = spi::Execute(sql.c_str(), static_cast<std::int64_t>(top_k));
    std::vector<CategoricalValue> out;
    for (const auto& row : rs) {
        if (row[0].IsNull()) continue;
        out.push_back({column, row[0].As<std::string>(), row[1].As<std::int64_t>()});
    }
    return out;
}

std::vector<std::string> ScanBooleanColumns(const std::string& table,
                                              const std::string& id_col_to_skip) {
    static constexpr const char* kQuery =
        "SELECT a.attname "
        "FROM pg_attribute a "
        "WHERE a.attrelid = $1::regclass "
        "  AND a.attnum > 0 "
        "  AND NOT a.attisdropped "
        "  AND format_type(a.atttypid, a.atttypmod) = 'boolean' "
        "ORDER BY a.attnum";
    auto rs = spi::Execute(kQuery, table);
    std::vector<std::string> out;
    for (const auto& row : rs) {
        const auto name = row[0].As<std::string>();
        if (name == id_col_to_skip) continue;
        out.push_back(name);
    }
    return out;
}

std::vector<std::string> ScanRealNumericColumns(const std::string& table,
                                                  const std::string& id_col_to_skip) {
    static constexpr const char* kQuery =
        "SELECT a.attname "
        "FROM pg_attribute a "
        "WHERE a.attrelid = $1::regclass "
        "  AND a.attnum > 0 "
        "  AND NOT a.attisdropped "
        "  AND format_type(a.atttypid, a.atttypmod) IN "
        "      ('integer', 'bigint', 'smallint', 'real', 'double precision', 'numeric') "
        "ORDER BY a.attnum";
    auto rs = spi::Execute(kQuery, table);
    std::vector<std::string> out;
    for (const auto& row : rs) {
        const auto name = row[0].As<std::string>();
        if (name == id_col_to_skip) continue;
        out.push_back(name);
    }
    return out;
}

std::vector<AtomConfig> GenerateDefaultAtomConfigs(const FnnConfig& cfg) {
    // Только content-based атомы (без ratings-percentile — это popularity-in-disguise).
    std::vector<AtomConfig> out;
    const std::string& rating = cfg.interactions.rating_col;
    constexpr int kCategoricalTopK = 5;

    if (cfg.items.has_value()) {
        auto item_real = ScanRealNumericColumns(cfg.items->table, cfg.items->id_col);
        ereport(NOTICE,
                (errmsg("recdb2/fnn: items_table '%s' — %zu numeric (gaussian + buckets) колонок",
                        cfg.items->table.c_str(), item_real.size())));
        for (const auto& col : item_real) {
            out.push_back({"item_" + col, AtomKind::ItemsColumn, "", col, "continuous", false,
                            "", "", "", "", true});
        }

        auto item_bool = ScanBooleanColumns(cfg.items->table, cfg.items->id_col);
        ereport(NOTICE,
                (errmsg("recdb2/fnn: items_table '%s' — %zu boolean колонок",
                        cfg.items->table.c_str(), item_bool.size())));
        for (const auto& col : item_bool) {
            out.push_back({"item_" + col, AtomKind::ItemsColumn, "", col, "0.5", false});
        }

        auto item_cat = ScanCategoricalColumns(cfg.items->table, cfg.items->id_col);
        ereport(NOTICE, (errmsg("recdb2/fnn: items_table '%s' — %zu categorical колонок",
                                 cfg.items->table.c_str(), item_cat.size())));
        for (const auto& col : item_cat) {
            auto vals = GetTopValues(cfg.items->table, col, kCategoricalTopK);
            for (const auto& v : vals) {
                AtomConfig a;
                a.name = "item_" + col + "_" + v.value;
                a.kind = AtomKind::ItemsColumn;
                a.column = col;
                a.threshold_spec = "0";
                a.categorical_value = v.value;
                out.push_back(std::move(a));
            }
        }
    }

    // Per-user content preference: атом user_likes_<col> на каждую item-bool колонку.
    if (cfg.items.has_value()) {
        auto bool_cols = ScanBooleanColumns(cfg.items->table, cfg.items->id_col);
        ereport(NOTICE, (errmsg("recdb2/fnn: per-user prefs (boolean) — %zu колонок",
                                 bool_cols.size())));
        for (const auto& bc : bool_cols) {
            AtomConfig a;
            a.name = "user_likes_" + bc;
            a.kind = AtomKind::UserAggregate;
            a.aggregate_fn = "avg";
            a.column = rating;
            a.threshold_spec = "p50";
            a.filter_item_column = bc;  // backward compat: пустой value = boolean = true
            out.push_back(std::move(a));
        }
    }

    if (cfg.users.has_value()) {
        auto user_real = ScanRealNumericColumns(cfg.users->table, cfg.users->id_col);
        ereport(NOTICE,
                (errmsg("recdb2/fnn: users_table '%s' — %zu numeric (gaussian) колонок",
                        cfg.users->table.c_str(), user_real.size())));
        for (const auto& col : user_real) {
            out.push_back({"user_" + col, AtomKind::UserColumn, "", col, "continuous", false,
                            "", "", "", "", true});
        }

        auto user_bool = ScanBooleanColumns(cfg.users->table, cfg.users->id_col);
        ereport(NOTICE,
                (errmsg("recdb2/fnn: users_table '%s' — %zu boolean колонок",
                        cfg.users->table.c_str(), user_bool.size())));
        for (const auto& col : user_bool) {
            out.push_back({"user_" + col, AtomKind::UserColumn, "", col, "0.5", false});
        }

        auto user_cat = ScanCategoricalColumns(cfg.users->table, cfg.users->id_col);
        ereport(NOTICE, (errmsg("recdb2/fnn: users_table '%s' — %zu categorical колонок",
                                 cfg.users->table.c_str(), user_cat.size())));
        for (const auto& col : user_cat) {
            auto vals = GetTopValues(cfg.users->table, col, kCategoricalTopK);
            for (const auto& v : vals) {
                AtomConfig a;
                a.name = "user_" + col + "_" + v.value;
                a.kind = AtomKind::UserColumn;
                a.column = col;
                a.threshold_spec = "0";
                a.categorical_value = v.value;
                out.push_back(std::move(a));
            }
        }
    }

    return out;
}

double ParsePercentileSpec(const std::string& spec) {
    if (spec.size() >= 2 && spec[0] == 'p') {
        const auto pct = std::stoi(spec.substr(1));
        if (pct < 1 || pct > 99) {
            ereport(ERROR,
                    (errmsg("recdb2/fnn: percentile spec '%s' out of range", spec.c_str())));
        }
        return pct / 100.0;
    }
    ereport(ERROR, (errmsg("recdb2/fnn: invalid threshold spec '%s'", spec.c_str())));
    __builtin_unreachable();
}

double ResolveAggregatePercentile(const FnnConfig& cfg, const AtomConfig& a) {
    const double q = ParsePercentileSpec(a.threshold_spec);
    const std::string& interactions_table = cfg.interactions.table;
    const std::string& user_col = cfg.interactions.user_col;
    const std::string& item_col = cfg.interactions.item_col;

    std::string group_col;
    switch (a.kind) {
        case AtomKind::ItemsAggregate:
            group_col = item_col;
            break;
        case AtomKind::UserAggregate:
            group_col = user_col;
            break;
        default:
            ereport(ERROR, (errmsg("recdb2/fnn: percentile spec only valid for aggregate atoms")));
    }

    const std::string inner =
        (a.column == "*") ? "count(*)::double precision"
                          : a.aggregate_fn + "(r." + a.column + ")::double precision";

    std::string from_clause = interactions_table + " r";
    std::string where_clause;
    if (!a.filter_item_column.empty() && cfg.items.has_value()) {
        from_clause += " JOIN " + cfg.items->table + " m ON m." + cfg.items->id_col +
                        " = r." + item_col;
        // пустой value → boolean=true; '=' → строковое равенство; иначе числовой оператор
        if (a.filter_item_value.empty()) {
            where_clause = " WHERE (m." + a.filter_item_column + ")::text = 'true'";
        } else if (a.filter_item_op.empty() || a.filter_item_op == "=") {
            std::string lit;
            lit.reserve(a.filter_item_value.size() + 2);
            lit += '\'';
            for (char c : a.filter_item_value) { if (c == '\'') lit += '\''; lit += c; }
            lit += '\'';
            where_clause = " WHERE m." + a.filter_item_column + " = " + lit;
        } else {
            where_clause = " WHERE (m." + a.filter_item_column + ")::double precision " +
                            a.filter_item_op + " " + a.filter_item_value;
        }
    }

    const std::string sql = "SELECT percentile_cont($1) WITHIN GROUP (ORDER BY agg_v) "
                            "FROM (SELECT " + inner + " AS agg_v FROM " + from_clause +
                            where_clause + " GROUP BY r." + group_col +
                            ") sub WHERE agg_v IS NOT NULL ";

    auto rs = spi::Execute(sql.c_str(), q);
    if (rs.IsEmpty() || rs[0][0].IsNull()) {
        ereport(NOTICE, (errmsg("recdb2/fnn: percentile NULL for atom '%s' — fallback 0.5",
                                 a.name.c_str())));
        return 0.5;
    }
    return rs[0][0].As<double>();
}

double ResolveColumnPercentile(const FnnConfig& cfg, const AtomConfig& a) {
    const double q = ParsePercentileSpec(a.threshold_spec);
    std::string source_table;
    switch (a.kind) {
        case AtomKind::ItemsColumn:
            if (!cfg.items.has_value()) {
                ereport(ERROR,
                        (errmsg("recdb2/fnn: items_column atom '%s' requires items_table",
                                a.name.c_str())));
            }
            source_table = cfg.items->table;
            break;
        case AtomKind::UserColumn:
            if (!cfg.users.has_value()) {
                ereport(ERROR,
                        (errmsg("recdb2/fnn: user_column atom '%s' requires users",
                                a.name.c_str())));
            }
            source_table = cfg.users->table;
            break;
        default:
            ereport(ERROR, (errmsg("recdb2/fnn: invalid kind for column percentile")));
    }
    // Унифицированный numeric expression — корректно обрабатывает boolean колонки.
    const std::string num_expr = "COALESCE(CASE WHEN (" + a.column +
                                  ")::text = 'true' THEN 1.0 WHEN (" + a.column +
                                  ")::text = 'false' THEN 0.0 END, NULLIF((" + a.column +
                                  ")::text, '')::double precision)";
    const std::string sql = "SELECT percentile_cont($1) WITHIN GROUP (ORDER BY " + num_expr +
                            ") FROM " + source_table + " WHERE " + a.column + " IS NOT NULL ";
    auto rs = spi::Execute(sql.c_str(), q);
    if (rs.IsEmpty() || rs[0][0].IsNull()) {
        ereport(ERROR, (errmsg("recdb2/fnn: column percentile NULL for atom '%s'",
                               a.name.c_str())));
    }
    return rs[0][0].As<double>();
}

double ResolveThreshold(const FnnConfig& cfg, const AtomConfig& a) {
    if (!a.threshold_spec.empty() && a.threshold_spec[0] == 'p') {
        switch (a.kind) {
            case AtomKind::ItemsAggregate:
            case AtomKind::UserAggregate:
                return ResolveAggregatePercentile(cfg, a);
            case AtomKind::ItemsColumn:
            case AtomKind::UserColumn:
                return ResolveColumnPercentile(cfg, a);
        }
    }
    try {
        return std::stod(a.threshold_spec);
    } catch (...) {
        ereport(ERROR,
                (errmsg("recdb2/fnn: cannot parse threshold '%s' for atom '%s'",
                        a.threshold_spec.c_str(), a.name.c_str())));
        __builtin_unreachable();
    }
}

}  // namespace

std::vector<AtomDef> BuildResolvedAtoms(const FnnConfig& cfg) {
    std::vector<AtomConfig> input = cfg.atoms;
    if (input.empty() || cfg.training.include_default_atoms) {
        auto defaults = GenerateDefaultAtomConfigs(cfg);
        input.insert(input.end(), defaults.begin(), defaults.end());
    }

    std::vector<AtomDef> out;
    out.reserve(input.size());
    for (const auto& a : input) {
        AtomDef def;
        def.name = a.name;
        def.kind = a.kind;
        def.aggregate_fn = a.aggregate_fn;
        def.column = a.column;
        def.negate = a.negate;
        def.categorical_value = a.categorical_value;
        def.filter_item_column = a.filter_item_column;
        def.filter_item_value = a.filter_item_value;
        def.filter_item_op = a.filter_item_op;
        def.membership = a.gaussian ? MembershipKind::Gaussian : MembershipKind::Binary;
        // Categorical атом: threshold/scale не используются, ветка в data_loader сразу
        // переходит на сравнение col = 'value'.
        if (!a.categorical_value.empty()) {
            def.threshold = 0.0;
            def.scale = 0.0;
            out.push_back(std::move(def));
            continue;
        }
        // Continuous: scale>0 => data_loader нормализует (col - min) / scale.
        if (a.threshold_spec == "continuous") {
            std::pair<double, double> range;
            switch (a.kind) {
                case AtomKind::ItemsColumn:
                    if (!cfg.items.has_value()) {
                        ereport(ERROR,
                                (errmsg("recdb2/fnn: continuous atom '%s' requires items_table",
                                        a.name.c_str())));
                    }
                    range = GetColumnRange(cfg.items->table, a.column);
                    break;
                case AtomKind::UserColumn:
                    if (!cfg.users.has_value()) {
                        ereport(ERROR,
                                (errmsg("recdb2/fnn: continuous atom '%s' requires users_table",
                                        a.name.c_str())));
                    }
                    range = GetColumnRange(cfg.users->table, a.column);
                    break;
                case AtomKind::ItemsAggregate:
                case AtomKind::UserAggregate:
                    range = GetAggregateRange(cfg, a.aggregate_fn, a.column, a.kind);
                    break;
            }
            def.threshold = range.first;
            const double scale = range.second - range.first;
            def.scale = (scale > 1e-9) ? scale : 1.0;
        } else {
            def.threshold = ResolveThreshold(cfg, a);
            def.scale = 0.0;
        }
        out.push_back(std::move(def));
    }
    return out;
}

std::string AggregateExprFor(const AtomDef& a) {
    if (a.column == "*") return "count(*)::double precision";
    return a.aggregate_fn + "(" + a.column + ")::double precision";
}

std::string AtomSqlExpression(const AtomDef& a, const std::string& items_alias,
                               const std::string& users_alias) {
    std::string value;
    switch (a.kind) {
        case AtomKind::ItemsAggregate:
            value = items_alias + "." + a.name + "_value";
            break;
        case AtomKind::UserAggregate:
            value = users_alias + "." + a.name + "_value";
            break;
        case AtomKind::ItemsColumn:
            value = items_alias + "." + a.column + "::double precision";
            break;
        case AtomKind::UserColumn:
            value = users_alias + "." + a.column + "::double precision";
            break;
    }
    char buf[64];
    snprintf(buf, sizeof(buf), "%.10g", a.threshold);
    return "(CASE WHEN " + value + " >= " + buf + " THEN 1.0 ELSE 0.0 END)";
}

}  // namespace recdb2::algorithm::fnn
