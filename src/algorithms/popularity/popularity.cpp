#include "popularity.hpp"
#include "src/algorithms/algorithm.hpp"
#include "src/spi/execute.hpp"
#include "src/core/queries.hpp"
#include "src/utils/json_utils.hpp"
#include "src/utils/query_builder.hpp"
#include <cstdint>

namespace recdb2::algorithm {

namespace {

std::string BuildQueryForTrain(const std::string& ratings_table, const std::string& item_col,
                               const std::string& rating_col) {
    return utils::QueryBuilder()
        .InsertInto("recdb2_predictions", {"model_id", "item_id", "score"})
        .Select({"$1", item_col + "::bigint", "AVG(" + rating_col + "::double precision)"})
        .From(ratings_table)
        .GroupBy(item_col)
        .Build();
}

std::string BuildRecommendQuery(const std::string& ratings_table, const std::string& user_col,
                                const std::string& item_col) {
    const auto excluded = utils::QueryBuilder()
                              .Select({item_col + "::bigint"})
                              .From(ratings_table)
                              .Where(user_col + "::bigint = $2")
                              .Build();

    return utils::QueryBuilder()
        .Select({"p.item_id", "p.score"})
        .From("recdb2_predictions p")
        .Where("p.model_id = $1")
        .AndWhere("p.item_id NOT IN (" + excluded + ")")
        .OrderBy("p.score", "DESC")
        .Limit("$3")
        .Build();
}

}  // namespace

std::vector<std::string> PopularityAlgorithm::RequiredConfigKeys() const {
    return {"ratings_table", "user_col", "item_col", "rating_col"};
}

void PopularityAlgorithm::Train(std::int64_t model_id, const std::string& config_json) {
    const auto ratings_table = utils::JsonGet(config_json, "ratings_table");
    const auto item_col = utils::JsonGet(config_json, "item_col");
    const auto rating_col = utils::JsonGet(config_json, "rating_col");

    spi::Execute(sql::kDeletePredictions, model_id);

    const auto query = BuildQueryForTrain(ratings_table, item_col, rating_col);
    spi::Execute(query.c_str(), model_id);
}

std::vector<Prediction> PopularityAlgorithm::Recommend(std::int64_t model_id, std::int64_t user_id,
                                                       int top_n, const std::string& config_json) {
    const auto ratings_table = utils::JsonGet(config_json, "ratings_table");
    const auto user_col = utils::JsonGet(config_json, "user_col");
    const auto item_col = utils::JsonGet(config_json, "item_col");

    const auto query = BuildRecommendQuery(ratings_table, user_col, item_col);

    auto result_set =
        spi::Execute(query.c_str(), model_id, user_id, static_cast<std::int64_t>(top_n));

    std::vector<Prediction> predictions;
    predictions.reserve(result_set.Size());
    for (const auto& row : result_set) {
        predictions.push_back(Prediction{
            .item_id = row[0].As<std::int64_t>(),
            .score = row[1].As<double>(),
        });
    }
    return predictions;
}

double PopularityAlgorithm::Score(std::int64_t model_id, std::int64_t, std::int64_t item_id,
                                    const std::string&) {
    static constexpr const char* kQuery =
        "SELECT score FROM recdb2_predictions WHERE model_id = $1 AND item_id = $2 ";
    auto rs = spi::Execute(kQuery, model_id, item_id);
    if (rs.IsEmpty() || rs[0][0].IsNull()) return 0.0;
    return rs[0][0].As<double>();
}

std::vector<ExplanationItem> PopularityAlgorithm::Explain(std::int64_t model_id, std::int64_t,
                                                          std::int64_t item_id,
                                                          const std::string&) {
    auto rs = spi::Execute(sql::kSelectPredictionRank, model_id, item_id);
    if (rs.IsEmpty()) {
        ereport(ERROR, (errmsg("recdb2: item_id=%lld has no prediction for model_id=%lld",
                               static_cast<long long>(item_id),
                               static_cast<long long>(model_id))));
    }

    const auto& row = rs.SingleRow();
    const double score = row[0].As<double>();
    const std::int64_t rank = row[1].As<std::int64_t>();
    const std::int64_t total = row[2].As<std::int64_t>();

    const double normalized_rank = total > 0 ? 1.0 - static_cast<double>(rank - 1) / total : 0.0;

    std::vector<ExplanationItem> out;
    out.reserve(2);
    out.push_back(ExplanationItem{
        .kind = "popularity_score",
        .label = "avg_rating",
        .contribution = score,
        .details_json = "{}",
    });
    out.push_back(ExplanationItem{
        .kind = "rank",
        .label = std::to_string(rank) + " of " + std::to_string(total),
        .contribution = normalized_rank,
        .details_json = "{\"rank\": " + std::to_string(rank) + ", \"total\": " +
                        std::to_string(total) + "}",
    });
    return out;
}

}  // namespace recdb2::algorithm