extern "C" {

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(recdb2_hello);
Datum recdb2_hello(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(recdb2_spi_ping);
Datum recdb2_spi_ping(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(recdb2_create_recommender);
Datum recdb2_create_recommender(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(recdb2_train);
Datum recdb2_train(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(recdb2_recommend);
Datum recdb2_recommend(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(recdb2_retrain);
Datum recdb2_retrain(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(recdb2_drop_recommender);
Datum recdb2_drop_recommender(PG_FUNCTION_ARGS);
}

#include "../core/service.hpp"
#include "../pg_spi/spi_session.hpp"

Datum recdb2_hello(PG_FUNCTION_ARGS) {
    recdb2::core::RecommenderService svc;
    const std::string message = svc.Hello();
    PG_RETURN_TEXT_P(cstring_to_text(message.c_str()));
}

Datum recdb2_spi_ping(PG_FUNCTION_ARGS) {
    recdb2::pg_spi::SpiSession::RunVoid([]() {});
    PG_RETURN_TEXT_P(cstring_to_text("recdb2: SPI ok"));
}

Datum recdb2_create_recommender(PG_FUNCTION_ARGS) {
    text* name_text = PG_GETARG_TEXT_PP(0);
    text* algo_text = PG_GETARG_TEXT_PP(1);
    text* cfg_text = PG_GETARG_TEXT_PP(2);

    char* name_c = text_to_cstring(name_text);
    char* algo_c = text_to_cstring(algo_text);
    char* cfg_c = text_to_cstring(cfg_text);

    std::int64_t id = recdb2::pg_spi::SpiSession::Run([&]() -> std::int64_t {
        recdb2::core::RecommenderService svc;
        return svc.CreateRecommender(name_c, algo_c, cfg_c);
    });

    PG_RETURN_INT64(id);
}

Datum recdb2_train(PG_FUNCTION_ARGS) {
    text* name_text = PG_GETARG_TEXT_PP(0);
    char* name_c = text_to_cstring(name_text);

    std::string result = recdb2::pg_spi::SpiSession::Run([&]() -> std::string {
        recdb2::core::RecommenderService svc;
        return svc.Train(name_c);
    });

    PG_RETURN_TEXT_P(cstring_to_text(result.c_str()));
}

Datum recdb2_recommend(PG_FUNCTION_ARGS) {
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;

    TupleDesc tupdesc;
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        ereport(ERROR, (errmsg("recdb2: function returning record called in context that cannot "
                               "accept type record")));
    }
    tupdesc = BlessTupleDesc(tupdesc);

    MemoryContext old_ctx = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    Tuplestorestate* tupstore =
        tuplestore_begin_heap(rsinfo->allowedModes & SFRM_Materialize_Random, false, work_mem);
    MemoryContextSwitchTo(old_ctx);

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    text* name_text = PG_GETARG_TEXT_PP(0);
    char* name_c = text_to_cstring(name_text);
    int64 user_id = PG_GETARG_INT64(1);
    int32 top_n = PG_GETARG_INT32(2);

    auto rows = recdb2::pg_spi::SpiSession::Run([&]() {
        recdb2::core::RecommenderService svc;
        return svc.Recommend(name_c, user_id, top_n);
    });

    for (const auto& rec : rows) {
        Datum values[2];
        bool nulls[2] = {false, false};
        values[0] = Int64GetDatum(rec.item_id);
        values[1] = Float8GetDatum(rec.score);
        HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
        tuplestore_puttuple(tupstore, tuple);
    }

    return (Datum)0;
}

Datum recdb2_retrain(PG_FUNCTION_ARGS) {
    text* name_text = PG_GETARG_TEXT_PP(0);
    char* name_c = text_to_cstring(name_text);

    std::string result = recdb2::pg_spi::SpiSession::Run([&]() -> std::string {
        recdb2::core::RecommenderService svc;
        return svc.Retrain(name_c);
    });

    PG_RETURN_TEXT_P(cstring_to_text(result.c_str()));
}

Datum recdb2_drop_recommender(PG_FUNCTION_ARGS) {
    text* name_text = PG_GETARG_TEXT_PP(0);
    char* name_c = text_to_cstring(name_text);

    std::string result = recdb2::pg_spi::SpiSession::Run([&]() -> std::string {
        recdb2::core::RecommenderService svc;
        return svc.Drop(name_c);
    });

    PG_RETURN_TEXT_P(cstring_to_text(result.c_str()));
}