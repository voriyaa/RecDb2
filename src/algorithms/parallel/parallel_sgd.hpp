// Алгоритмо-независимый движок data-parallel Local SGD. PG-free заголовок: без
// символов Postgres, включается в любой чистый C++ TU (привязка к PG — в .cpp).

#pragma once

#include <cstddef>
#include <memory>
#include <random>
#include <vector>

namespace recdb2::parallel {

// Контракт градиентной модели: реализация хранит свои параметры и моменты Adam
// между эпохами; движок их не трогает.
struct ParallelSgdModel {
    virtual ~ParallelSgdModel() = default;

    virtual int ParamVectorSize() const = 0;

    // Раскладка плоского вектора должна быть стабильной (усреднение поэлементное);
    // ImportParams не сбрасывает оптимизатор.
    virtual void ExportParams(double* out) const = 0;
    virtual void ImportParams(const double* in) = 0;

    // Одна эпоха SGD по приватному шарду воркера. features/targets — read-only в DSM;
    // shard_idx можно перемешивать на месте.
    virtual void TrainShardEpoch(int epoch, int total_epochs, const float* features,
                                 const double* targets, int* shard_idx, int shard_len,
                                 int n_features, int batch_size, std::mt19937_64& rng) = 0;
};

struct ParallelSgdDims {
    int n_samples = 0;
    int n_features = 0;
    int n_params = 0;
    int epochs = 0;
    int batch_size = 1;
    long random_seed = 0;
    int algo_id = 0;
};

// Фабрика модели (зовётся в каждом воркере); nullptr -> ошибка.
using ModelFactory = std::unique_ptr<ParallelSgdModel> (*)(const ParallelSgdDims& dims,
                                                           const void* hyper,
                                                           std::size_t hyper_len);

// Регистрировать из статического инициализатора TU алгоритма (отрабатывает при
// загрузке recdb2.dylib в каждом воркере, до вызова фабрики).
void RegisterParallelSgdModel(int algo_id, ModelFactory factory);

struct ParallelSgdSpec {
    int algo_id = 0;
    int n_samples = 0;
    int n_features = 0;
    int epochs = 1;
    int batch_size = 1;
    long random_seed = 0;
    const float* features = nullptr;    // [n_samples * n_features], владелец — вызывающий
    const double* targets = nullptr;    // [n_samples], владелец — вызывающий
    std::vector<double> init_params;    // [n_params] — стартовая точка
    std::vector<unsigned char> hyper;   // непрозрачный алгоритм-специфичный блоб
    int requested_workers = 2;
};

// Точка входа лидера: поднимает воркеров, гоняет Local SGD, возвращает усреднённый
// вектор параметров. out_compute_seconds (опц.) <- SGD wall-clock без заполнения DSM.
std::vector<double> RunDataParallelSgd(const ParallelSgdSpec& spec,
                                       double* out_compute_seconds = nullptr);

}  // namespace recdb2::parallel
