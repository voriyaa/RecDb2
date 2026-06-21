#include "parallel_sgd.hpp"

extern "C" {
#include "postgres.h"

#include "access/parallel.h"
#include "access/xact.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/barrier.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "utils/elog.h"
#include "utils/wait_classes.h"
}

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <numeric>
#include <random>
#include <unordered_map>
#include <vector>

namespace {

constexpr uint64 kKeyHeader = 1;
constexpr uint64 kKeyFeatures = 2;
constexpr uint64 kKeyTargets = 3;
constexpr uint64 kKeyHyper = 4;
constexpr uint64 kKeyInit = 5;
constexpr uint64 kKeyGlobal = 6;
constexpr uint64 kKeySlots = 7;

struct ParallelSgdHeader {
    std::int32_t n_samples;
    std::int32_t n_features;
    std::int32_t n_params;
    std::int32_t epochs;
    std::int32_t batch_size;
    std::int32_t n_launched;
    std::int32_t algo_id;
    std::int32_t hyper_len;
    std::int64_t random_seed;
    pg_atomic_uint32 ready;
    Barrier barrier;
};

std::unordered_map<int, recdb2::parallel::ModelFactory>& Registry() {
    static std::unordered_map<int, recdb2::parallel::ModelFactory> registry;
    return registry;
}

std::unique_ptr<recdb2::parallel::ParallelSgdModel> MakeModel(
    int algo_id, const recdb2::parallel::ParallelSgdDims& dims, const void* hyper,
    std::size_t hyper_len) {
    auto& reg = Registry();
    const auto it = reg.find(algo_id);
    if (it == reg.end()) return nullptr;
    return it->second(dims, hyper, hyper_len);
}

}

namespace recdb2::parallel {

void RegisterParallelSgdModel(int algo_id, ModelFactory factory) {
    Registry()[algo_id] = factory;
}

}

extern "C" PGDLLEXPORT void recdb2_parallel_sgd_worker(dsm_segment* seg, shm_toc* toc) {
    using namespace recdb2::parallel;
    (void)seg;
    try {
        auto* h = static_cast<ParallelSgdHeader*>(shm_toc_lookup(toc, kKeyHeader, false));
        const auto* features = static_cast<const float*>(shm_toc_lookup(toc, kKeyFeatures, false));
        const auto* targets = static_cast<const double*>(shm_toc_lookup(toc, kKeyTargets, false));
        const void* hyper = shm_toc_lookup(toc, kKeyHyper, false);
        const auto* init = static_cast<const double*>(shm_toc_lookup(toc, kKeyInit, false));
        auto* global = static_cast<double*>(shm_toc_lookup(toc, kKeyGlobal, false));
        auto* slots = static_cast<double*>(shm_toc_lookup(toc, kKeySlots, false));

        while (pg_atomic_read_u32(&h->ready) == 0) {
            CHECK_FOR_INTERRUPTS();
            pg_usleep(500L);
        }
        pg_read_barrier();

        const int n = h->n_launched;
        const int widx = ParallelWorkerNumber;
        if (widx < 0 || widx >= n) return;

        const int n_params = h->n_params;
        const int n_features = h->n_features;
        const int epochs = h->epochs;
        const int batch_size = std::max(1, h->batch_size);

        ParallelSgdDims dims;
        dims.n_samples = h->n_samples;
        dims.n_features = n_features;
        dims.n_params = n_params;
        dims.epochs = epochs;
        dims.batch_size = batch_size;
        dims.random_seed = h->random_seed;
        dims.algo_id = h->algo_id;

        std::unique_ptr<ParallelSgdModel> model =
            MakeModel(h->algo_id, dims, hyper, static_cast<std::size_t>(h->hyper_len));
        if (!model) {
            ereport(ERROR,
                    (errmsg("recdb2/parallel: нет зарегистрированной фабрики для algo_id=%d",
                            h->algo_id)));
        }
        model->ImportParams(init);

        const std::int64_t S = h->n_samples;
        const int lo = static_cast<int>(S * widx / n);
        const int hi = static_cast<int>(S * (widx + 1) / n);
        std::vector<int> idx(static_cast<std::size_t>(std::max(0, hi - lo)));
        std::iota(idx.begin(), idx.end(), lo);

        std::mt19937_64 rng(static_cast<std::uint64_t>(h->random_seed) ^
                            (0x9e3779b97f4a7c15ULL * static_cast<std::uint64_t>(widx + 1)));

        double* my_slot = slots + static_cast<std::size_t>(widx) * n_params;

        for (int epoch = 0; epoch < epochs; ++epoch) {
            model->TrainShardEpoch(epoch, epochs, features, targets, idx.data(),
                                   static_cast<int>(idx.size()), n_features, batch_size, rng);

            model->ExportParams(my_slot);

            BarrierArriveAndWait(&h->barrier, PG_WAIT_EXTENSION);
            if (widx == 0) {
                for (int c = 0; c < n_params; ++c) {
                    double s = 0.0;
                    for (int w = 0; w < n; ++w) {
                        s += slots[static_cast<std::size_t>(w) * n_params + c];
                    }
                    global[c] = s / static_cast<double>(n);
                }
            }
            BarrierArriveAndWait(&h->barrier, PG_WAIT_EXTENSION);

            model->ImportParams(global);
        }
    } catch (const std::exception& e) {
        ereport(ERROR,
                (errmsg("recdb2/parallel: воркер %d упал: %s", ParallelWorkerNumber, e.what())));
    } catch (...) {
        ereport(ERROR, (errmsg("recdb2/parallel: воркер %d — неизвестное C++ исключение",
                                ParallelWorkerNumber)));
    }
}

namespace recdb2::parallel {

std::vector<double> RunDataParallelSgd(const ParallelSgdSpec& spec, double* out_compute_seconds) {
    const int n_params = static_cast<int>(spec.init_params.size());
    const int n_samples = spec.n_samples;
    const int n_features = spec.n_features;
    const int epochs = std::max(1, spec.epochs);
    const int requested = std::max(2, std::min(spec.requested_workers, 16));

    EnterParallelMode();
    ParallelContext* pcxt =
        CreateParallelContext("recdb2", "recdb2_parallel_sgd_worker", requested);

    const Size features_bytes = sizeof(float) * static_cast<Size>(n_samples) * n_features;
    const Size targets_bytes = sizeof(double) * static_cast<Size>(n_samples);
    const Size hyper_bytes = std::max<Size>(spec.hyper.size(), Size(1));
    const Size params_bytes = sizeof(double) * static_cast<Size>(n_params);
    const Size slots_bytes = params_bytes * static_cast<Size>(requested);

    shm_toc_estimate_chunk(&pcxt->estimator, sizeof(ParallelSgdHeader));
    shm_toc_estimate_chunk(&pcxt->estimator, features_bytes);
    shm_toc_estimate_chunk(&pcxt->estimator, targets_bytes);
    shm_toc_estimate_chunk(&pcxt->estimator, hyper_bytes);
    shm_toc_estimate_chunk(&pcxt->estimator, params_bytes);
    shm_toc_estimate_chunk(&pcxt->estimator, params_bytes);
    shm_toc_estimate_chunk(&pcxt->estimator, slots_bytes);
    shm_toc_estimate_keys(&pcxt->estimator, 7);

    InitializeParallelDSM(pcxt);

    auto* h = static_cast<ParallelSgdHeader*>(
        shm_toc_allocate(pcxt->toc, sizeof(ParallelSgdHeader)));
    h->n_samples = n_samples;
    h->n_features = n_features;
    h->n_params = n_params;
    h->epochs = epochs;
    h->batch_size = std::max(1, spec.batch_size);
    h->n_launched = 0;
    h->algo_id = spec.algo_id;
    h->hyper_len = static_cast<std::int32_t>(spec.hyper.size());
    h->random_seed = spec.random_seed;
    pg_atomic_init_u32(&h->ready, 0);
    shm_toc_insert(pcxt->toc, kKeyHeader, h);

    auto* features = static_cast<float*>(shm_toc_allocate(pcxt->toc, features_bytes));
    std::memcpy(features, spec.features, features_bytes);
    shm_toc_insert(pcxt->toc, kKeyFeatures, features);

    auto* targets = static_cast<double*>(shm_toc_allocate(pcxt->toc, targets_bytes));
    std::memcpy(targets, spec.targets, targets_bytes);
    shm_toc_insert(pcxt->toc, kKeyTargets, targets);

    auto* hyper = static_cast<unsigned char*>(shm_toc_allocate(pcxt->toc, hyper_bytes));
    if (!spec.hyper.empty()) std::memcpy(hyper, spec.hyper.data(), spec.hyper.size());
    shm_toc_insert(pcxt->toc, kKeyHyper, hyper);

    auto* init = static_cast<double*>(shm_toc_allocate(pcxt->toc, params_bytes));
    std::memcpy(init, spec.init_params.data(), params_bytes);
    shm_toc_insert(pcxt->toc, kKeyInit, init);

    auto* global = static_cast<double*>(shm_toc_allocate(pcxt->toc, params_bytes));
    std::memcpy(global, init, params_bytes);
    shm_toc_insert(pcxt->toc, kKeyGlobal, global);

    auto* slots = static_cast<double*>(shm_toc_allocate(pcxt->toc, slots_bytes));
    std::memset(slots, 0, slots_bytes);
    shm_toc_insert(pcxt->toc, kKeySlots, slots);

    const auto compute_t0 = std::chrono::steady_clock::now();
    LaunchParallelWorkers(pcxt);
    const int launched = pcxt->nworkers_launched;
    if (launched < 1) {
        DestroyParallelContext(pcxt);
        ExitParallelMode();
        ereport(ERROR, (errmsg("recdb2/parallel: не удалось запустить ни одного воркера "
                                "(запрошено %d); увеличьте max_parallel_workers или обучайте "
                                "серийно (parallel_workers=1)",
                                requested)));
    }

    BarrierInit(&h->barrier, launched);
    h->n_launched = launched;
    pg_write_barrier();
    pg_atomic_write_u32(&h->ready, 1);

    ereport(NOTICE, (errmsg("recdb2/parallel: %d воркеров запущено (запрошено %d), %d эпох, "
                            "%d примеров в DSM (%.1f МБ features), algo_id=%d",
                            launched, requested, epochs, n_samples,
                            static_cast<double>(features_bytes) / (1024.0 * 1024.0), spec.algo_id)));

    WaitForParallelWorkersToFinish(pcxt);
    const double compute_seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - compute_t0).count();
    ereport(NOTICE,
            (errmsg("recdb2/parallel: SGD wall-clock %.2f s (%d эпох, %d воркеров, launch→finish)",
                    compute_seconds, epochs, launched)));

    std::vector<double> result(global, global + n_params);

    DestroyParallelContext(pcxt);
    ExitParallelMode();

    if (out_compute_seconds) *out_compute_seconds = compute_seconds;
    return result;
}

}
