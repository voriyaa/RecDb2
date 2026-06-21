#pragma once

#include <cstddef>
#include <memory>
#include <random>
#include <vector>

namespace recdb2::parallel {

struct ParallelSgdModel {
    virtual ~ParallelSgdModel() = default;

    virtual int ParamVectorSize() const = 0;

    virtual void ExportParams(double* out) const = 0;
    virtual void ImportParams(const double* in) = 0;

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

using ModelFactory = std::unique_ptr<ParallelSgdModel> (*)(const ParallelSgdDims& dims,
                                                           const void* hyper,
                                                           std::size_t hyper_len);

void RegisterParallelSgdModel(int algo_id, ModelFactory factory);

struct ParallelSgdSpec {
    int algo_id = 0;
    int n_samples = 0;
    int n_features = 0;
    int epochs = 1;
    int batch_size = 1;
    long random_seed = 0;
    const float* features = nullptr;
    const double* targets = nullptr;
    std::vector<double> init_params;
    std::vector<unsigned char> hyper;
    int requested_workers = 2;
};

std::vector<double> RunDataParallelSgd(const ParallelSgdSpec& spec,
                                       double* out_compute_seconds = nullptr);

}
