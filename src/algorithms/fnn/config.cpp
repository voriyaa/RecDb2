#include "config.hpp"

#include "src/spi/execute.hpp"
#include "src/utils/json_utils.hpp"
#include "src/utils/validate.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

namespace recdb2::algorithm::fnn {

namespace {

void ValidateAggregateFn(const std::string& fn) {
    if (fn == "avg" || fn == "count" || fn == "min" || fn == "max" || fn == "sum" ||
        fn == "stddev" || fn == "stddev_samp" || fn == "stddev_pop")
        return;
    ereport(ERROR, (errmsg("recdb2/fnn: unsupported aggregate fn '%s'", fn.c_str())));
}

InteractionsConfig ParseInteractions(const std::string& config_json) {
    static constexpr const char* kQuery =
        "SELECT $1::jsonb -> 'interactions' ->> 'table', "
        "       $1::jsonb -> 'interactions' ->> 'user_col', "
        "       $1::jsonb -> 'interactions' ->> 'item_col', "
        "       $1::jsonb -> 'interactions' ->> 'rating_col', "
        "       $1::jsonb -> 'interactions' ->> 'ts_col' ";
    auto rs = spi::Execute(kQuery, config_json);
    if (rs.IsEmpty() || rs[0][0].IsNull()) {
        ereport(ERROR, (errmsg("recdb2/fnn: 'interactions' block is required")));
    }
    const auto& row = rs.SingleRow();
    InteractionsConfig ic;
    ic.table = row[0].As<std::string>();
    ic.user_col = row[1].As<std::string>();
    ic.item_col = row[2].As<std::string>();
    if (row[3].IsNull()) {
        ereport(ERROR, (errmsg("recdb2/fnn: interactions.rating_col is required")));
    }
    ic.rating_col = row[3].As<std::string>();
    if (!row[4].IsNull()) ic.ts_col = row[4].As<std::string>();
    utils::ValidateIdentifier(ic.table, "fnn.interactions.table");
    utils::ValidateIdentifier(ic.user_col, "fnn.interactions.user_col");
    utils::ValidateIdentifier(ic.item_col, "fnn.interactions.item_col");
    utils::ValidateIdentifier(ic.rating_col, "fnn.interactions.rating_col");
    if (!ic.ts_col.empty()) utils::ValidateIdentifier(ic.ts_col, "fnn.interactions.ts_col");
    return ic;
}

std::optional<ItemsConfig> ParseItems(const std::string& config_json) {
    static constexpr const char* kQuery =
        "SELECT COALESCE($1::jsonb ->> 'items_table', $1::jsonb -> 'items' ->> 'table'), "
        "       COALESCE($1::jsonb ->> 'items_id_col', $1::jsonb -> 'items' ->> 'id_col') ";
    auto rs = spi::Execute(kQuery, config_json);
    if (rs.IsEmpty() || rs[0][0].IsNull()) return std::nullopt;
    ItemsConfig ic;
    ic.table = rs[0][0].As<std::string>();
    ic.id_col = rs[0][1].IsNull() ? "id" : rs[0][1].As<std::string>();
    utils::ValidateIdentifier(ic.table, "fnn.items_table");
    utils::ValidateIdentifier(ic.id_col, "fnn.items_id_col");
    return ic;
}

std::optional<UsersConfig> ParseUsers(const std::string& config_json) {
    static constexpr const char* kQuery =
        "SELECT COALESCE($1::jsonb ->> 'users_table', $1::jsonb -> 'users' ->> 'table'), "
        "       COALESCE($1::jsonb ->> 'users_id_col', $1::jsonb -> 'users' ->> 'id_col') ";
    auto rs = spi::Execute(kQuery, config_json);
    if (rs.IsEmpty() || rs[0][0].IsNull()) return std::nullopt;
    UsersConfig uc;
    uc.table = rs[0][0].As<std::string>();
    uc.id_col = rs[0][1].IsNull() ? "id" : rs[0][1].As<std::string>();
    utils::ValidateIdentifier(uc.table, "fnn.users_table");
    utils::ValidateIdentifier(uc.id_col, "fnn.users_id_col");
    return uc;
}

void ParseAtoms(FnnConfig& cfg, const std::string& config_json) {
    static constexpr const char* kQuery =
        "SELECT a->>'name', a->>'kind', a->>'fn', a->>'col', a->>'threshold', "
        "       COALESCE((a->>'negate')::boolean, false), "
        "       a->>'categorical_value', "
        "       a->>'filter_item_column', "
        "       a->>'filter_item_value', "
        "       a->>'filter_item_op' "
        "FROM jsonb_array_elements($1::jsonb -> 'atoms') AS a ";
    auto rs = spi::Execute(kQuery, config_json);
    cfg.atoms.reserve(rs.Size());
    for (const auto& row : rs) {
        AtomConfig a;
        a.name = row[0].As<std::string>();
        a.kind = ParseAtomKind(row[1].As<std::string>());
        if (!row[2].IsNull()) a.aggregate_fn = row[2].As<std::string>();
        if (!row[3].IsNull()) a.column = row[3].As<std::string>();
        a.threshold_spec = row[4].IsNull() ? "p75" : row[4].As<std::string>();
        a.negate = row[5].As<bool>();
        if (!row[6].IsNull()) a.categorical_value = row[6].As<std::string>();
        if (!row[7].IsNull()) a.filter_item_column = row[7].As<std::string>();
        if (!row[8].IsNull()) a.filter_item_value = row[8].As<std::string>();
        if (!row[9].IsNull()) a.filter_item_op = row[9].As<std::string>();

        switch (a.kind) {
            case AtomKind::ItemsAggregate:
            case AtomKind::UserAggregate:
                if (a.aggregate_fn.empty()) {
                    ereport(ERROR, (errmsg("recdb2/fnn: atom '%s' (aggregate) requires 'fn'",
                                           a.name.c_str())));
                }
                ValidateAggregateFn(a.aggregate_fn);
                if (a.aggregate_fn == "count" && (a.column.empty() || a.column == "*")) {
                    a.column = "*";
                } else {
                    if (a.column.empty()) {
                        ereport(ERROR, (errmsg("recdb2/fnn: atom '%s' (aggregate) requires 'col'",
                                               a.name.c_str())));
                    }
                    utils::ValidateIdentifier(a.column, "fnn.atoms[].col");
                }
                break;
            case AtomKind::ItemsColumn:
            case AtomKind::UserColumn:
                if (a.column.empty()) {
                    ereport(ERROR, (errmsg("recdb2/fnn: atom '%s' (column) requires 'col'",
                                           a.name.c_str())));
                }
                utils::ValidateIdentifier(a.column, "fnn.atoms[].col");
                break;
        }
        cfg.atoms.push_back(std::move(a));
    }
}

void ParseTraining(FnnConfig& cfg, const std::string& config_json) {
    static constexpr const char* kQuery =
        "SELECT COALESCE(($1::jsonb -> 'training' ->> 'n_rules')::int, 4), "
        "       COALESCE(($1::jsonb -> 'training' ->> 'epochs')::int, 50), "
        "       COALESCE(($1::jsonb -> 'training' ->> 'learning_rate')::double precision, 0.05), "
        "       COALESCE(($1::jsonb -> 'training' ->> 'batch_size')::int, 1024), "
        "       COALESCE(($1::jsonb -> 'training' ->> 'test_split')::double precision, 0.2), "
        "       COALESCE(($1::jsonb -> 'training' ->> 'random_seed')::bigint, 42), "
        "       COALESCE(($1::jsonb -> 'training' ->> 'max_train_samples')::int, 2000000), "
        "       COALESCE(($1::jsonb -> 'training' ->> 'include_default_atoms')::boolean, true), "
        "       COALESCE(($1::jsonb -> 'training' ->> 'warm_start')::boolean, true), "
        "       COALESCE(($1::jsonb -> 'training' ->> 'warm_start_lr_scale')::double precision, 0.3), "
        "       COALESCE(($1::jsonb -> 'training' ->> 'warm_start_epoch_scale')::double precision, 0.3), "
        "       COALESCE(($1::jsonb -> 'training' ->> 'n_slots')::int, 4), "
        "       COALESCE(($1::jsonb -> 'training' ->> 'tau_start')::double precision, 1.5), "
        "       COALESCE(($1::jsonb -> 'training' ->> 'tau_end')::double precision, 0.05), "
        "       COALESCE(($1::jsonb -> 'training' ->> 'lambda_diversity')::double precision, 0.3), "
        "       COALESCE(($1::jsonb -> 'training' ->> 'parallel_workers')::int, 1) ";
    auto rs = spi::Execute(kQuery, config_json);
    const auto& r = rs.SingleRow();
    cfg.training.n_rules = static_cast<int>(r[0].As<std::int64_t>());
    cfg.training.epochs = static_cast<int>(r[1].As<std::int64_t>());
    cfg.training.learning_rate = r[2].As<double>();
    cfg.training.batch_size = static_cast<int>(r[3].As<std::int64_t>());
    cfg.training.test_split = r[4].As<double>();
    cfg.training.random_seed = static_cast<long>(r[5].As<std::int64_t>());
    cfg.training.max_train_samples = static_cast<int>(r[6].As<std::int64_t>());
    cfg.training.include_default_atoms = r[7].As<bool>();
    cfg.training.warm_start = r[8].As<bool>();
    cfg.training.warm_start_lr_scale = r[9].As<double>();
    cfg.training.warm_start_epoch_scale = r[10].As<double>();
    cfg.training.n_slots = static_cast<int>(r[11].As<std::int64_t>());
    cfg.training.tau_start = r[12].As<double>();
    cfg.training.tau_end = r[13].As<double>();
    cfg.training.lambda_diversity = r[14].As<double>();
    cfg.training.parallel_workers = static_cast<int>(r[15].As<std::int64_t>());

    if (cfg.training.n_slots <= 0) {
        ereport(ERROR, (errmsg("recdb2/fnn: n_slots must be > 0 (got %d)",
                               cfg.training.n_slots)));
    }
}

}  // namespace

FnnConfig ParseFnnConfig(const std::string& config_json) {
    FnnConfig cfg;
    cfg.interactions = ParseInteractions(config_json);
    cfg.items = ParseItems(config_json);
    cfg.users = ParseUsers(config_json);
    ParseAtoms(cfg, config_json);
    ParseTraining(cfg, config_json);

    if (cfg.training.n_rules < 1 || cfg.training.n_rules > 32) {
        ereport(ERROR,
                (errmsg("recdb2/fnn: n_rules=%d (must be 1..32)", cfg.training.n_rules)));
    }

    return cfg;
}

}  // namespace recdb2::algorithm::fnn
