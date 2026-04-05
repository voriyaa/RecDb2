#include "service.hpp"
#include "../pg_spi/models_repository_spi.hpp"
#include "src/algorithms/algorithm_factory.hpp"
#include "src/pg_spi/execute.hpp"
#include "src/sql/queries.hpp"
#include "src/utils/json_utils.hpp"
#include <string>

namespace recdb2::core {

namespace {

struct ModelInfo {
    std::int64_t id;
    std::string name;
    std::string algorithm;
    std::string state;
    std::string config_json;
};

ModelInfo LoadModel(const std::string& model_name) {
    auto result_set = pg_spi::Execute(sql::kSelectModelByName, model_name);
    if (result_set.IsEmpty()) {
        ereport(ERROR, (errmsg("recdb2: model '%s' not found", model_name.c_str())));
    }
    const auto& row = result_set[0];
    return ModelInfo{.id = row[0].As<std::int64_t>(),
                     .name = row[1].As<std::string>(),
                     .algorithm = row[2].As<std::string>(),
                     .state = row[3].As<std::string>(),
                     .config_json = row[4].As<std::string>()};
}

void SetModelState(std::int64_t id, const std::string& state,
                   const std::optional<std::string>& error = std::nullopt) {
    recdb2::pg_spi::Execute(recdb2::sql::kUpdateModelState, id, state, error);
}

}  // namespace

std::string RecommenderService::Hello() const {
    return "recdb2: hello";
}

std::int64_t RecommenderService::CreateRecommender(const std::string& name,
                                                   const std::string& algorithm,
                                                   const std::string& config_json_text) const {
    auto algo = algorithm::CreateAlgorithm(algorithm);

    for (const auto& key : algo->RequiredConfigKeys()) {
        auto value_opt = utils::JsonGetOptional(config_json_text, key.c_str());
        if (!value_opt.has_value()) {
            ereport(ERROR, (errmsg("recdb2: algorithm '%s' requires config key '%s'",
                                   algorithm.c_str(), key.c_str())));
        }
    }

    pg_spi::ModelsRepositorySpi repo;
    return repo.InsertModel(name, algorithm, config_json_text);
}

std::string RecommenderService::Train(const std::string& name) const {
    auto model = LoadModel(name);

    if (model.state != "created" && model.state != "failed") {
        ereport(ERROR,
                (errmsg("recdb2: model '%s' is in state '%s', expected 'created' or 'failed'",
                        name.c_str(), model.state.c_str())));
    }

    SetModelState(model.id, "training");

    auto algorithm_ptr = algorithm::CreateAlgorithm(model.algorithm);
    algorithm_ptr->Train(model.id, model.config_json);

    SetModelState(model.id, "ready");
    return "recdb2: model '" + name + "' trained successfully";
}

std::vector<algorithm::Prediction> RecommenderService::Recommend(const std::string& name,
                                                                 std::int64_t user_id,
                                                                 int top_n) const {
    auto model = LoadModel(name);

    if (model.state != "ready") {
        ereport(ERROR, (errmsg("recdb2: model '%s' is not ready (state='%s')", name.c_str(),
                               model.state.c_str())));
    }

    auto algorithm_ptr = algorithm::CreateAlgorithm(model.algorithm);
    return algorithm_ptr->Recommend(model.id, user_id, top_n, model.config_json);
}

std::string RecommenderService::Retrain(const std::string& name) const {
    auto model = LoadModel(name);

    if (model.state != "ready" && model.state != "failed") {
        ereport(ERROR, (errmsg("recdb2: model '%s' is in state '%s', expected 'ready' or 'failed'",
                               name.c_str(), model.state.c_str())));
    }

    SetModelState(model.id, "training");

    auto algo = algorithm::CreateAlgorithm(model.algorithm);
    algo->Train(model.id, model.config_json);

    SetModelState(model.id, "ready");
    return "recdb2: model '" + name + "' retrained successfully";
}

std::string RecommenderService::Drop(const std::string& name) const {
    auto model = LoadModel(name);
    pg_spi::Execute(recdb2::sql::kDeleteModel, name);
    return "recdb2: model '" + name + "' dropped";
}

}  // namespace recdb2::core
