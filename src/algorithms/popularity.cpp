#include "popularity.hpp"
#include "src/algorithms/algorithm.hpp"
#include "src/pg_spi/execute.hpp"
#include "src/sql/queries.hpp"
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

    pg_spi::Execute(sql::kDeletePredictions, model_id);

    const auto query = BuildQueryForTrain(ratings_table, item_col, rating_col);
    pg_spi::Execute(query.c_str(), model_id);
}

std::vector<Prediction> PopularityAlgorithm::Recommend(std::int64_t model_id, std::int64_t user_id,
                                                       int top_n, const std::string& config_json) {
    const auto ratings_table = utils::JsonGet(config_json, "ratings_table");
    const auto user_col = utils::JsonGet(config_json, "user_col");
    const auto item_col = utils::JsonGet(config_json, "item_col");

    const auto query = BuildRecommendQuery(ratings_table, user_col, item_col);

    auto result_set =
        pg_spi::Execute(query.c_str(), model_id, user_id, static_cast<std::int64_t>(top_n));

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

}  // namespace recdb2::algorithm